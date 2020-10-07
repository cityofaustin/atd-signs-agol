import collections
import logging
import math
import operator
from pprint import pprint as print
import requests

import arcgis
import knackpy
from shapely import wkt as shapely_wkt

from config import (
    AGOL_SERVICES,
    AUTH,
    APP_ID,
    API_KEY,
    KNACK,
    FIELD_MAP,
)

SR_ID = 4326  # wgs 1984
WORK_ORDER_ID_FIELD = "field_3214"
WORK_ORDER_STATUS_FIELD = "field_3265"
SIGN_LOCATION_ID_FIELD = "field_3297"


def create_geom_filter(geometry_dict, sr):
    """ create an intersection filter for spatial queries """
    geometry_dict["spatialReference"] = sr
    geom = arcgis.geometry.Geometry(geometry_dict)
    try:
        # unclear when a geom might be invalid, but it did once during testing,
        # possibly due to an incorrect spatial reference
        assert geom.is_valid()
    except AssertionError:
        raise ValueError(f"Invalid feature geometry: {geometry_dict}")
    return arcgis.geometry.filters.intersects(geom, sr=sr)


def find_nearest_feature(input_feature, search_features):
    """ all features must have the same spatial reference """
    input_feature_geom = arcgis.geometry.Geometry(input_feature.geometry)
    input_feature_shape = shapely_wkt.loads(input_feature_geom.WKT)
    nearest_feature = None
    distance_shortest = math.inf

    for search_feature in search_features:
        search_geom = arcgis.geometry.Geometry(search_feature.geometry)
        search_shape = shapely_wkt.loads(search_geom.WKT)
        distance_current = input_feature_shape.distance(search_shape)
        if distance_current < distance_shortest:
            distance_shortest = distance_current
            nearest_feature = search_feature

    return nearest_feature, distance_shortest


def block_range(segments):
    blk_start = min([s.attributes["LEFT_BLOCK_FROM"] for s in segments])
    blk_end = max([s.attributes["LEFT_BLOCK_TO"] for s in segments])
    # COA does not round block start/end. so let's do that manually to the nearest 100
    blk_start_round = math.floor(blk_start / 100) * 100
    blk_end_round = math.ceil(blk_end / 100) * 100
    return blk_start_round, blk_end_round


def parse_editor_name(agol_editor_value):
    # our AGOL usernames look like this: "John.Clary@austintexas.gov_austin"
    if agol_editor_value.lower() == "atd_publisher":
        # todo: handle this case
        return "john.clary@austintexas.gov"
    return agol_editor_value.replace("_austin", "").lower()


def get_user_index(app, obj_id, email_field_name="Email"):
    """ fetch knack account info and return a dict where each key as an email address
    whose value is a knack record id"""
    user_records = app.get(obj_id)
    return {user[email_field_name]["email"]: user["id"] for user in user_records}


def map_fields(data, field_map):
    """ will fail by design if any attributes in field map are missing from data """
    return {
        knack_field_name: data[agol_field_name]
        for agol_field_name, knack_field_name in field_map.items()
    }


def construct_engineer_note(data):
    action_value = data["INSTRUCTIONS"]
    action = f'Action: {data["INSTRUCTIONS"]}\n'
    comments = f'Note: {data["COMMENTS"]}\n' if data["COMMENTS"] else ""
    current_spd = f'Speed Limit: {data["CURRENT_SPEED_LIMIT"]}\n'
    future_spd = f'Speed limit: {data["FUTURE_SPEED_LIMIT"]}\n'
    custom_sign = f'Custom sign: {data["CUSTOM_SIGN"]}\n' if data["CUSTOM_SIGN"] else ""
    sign_type = f'Size: {data["SIGN_TYPE"]}\n'

    if action_value == "REPLACE" or action_value == "INSTALL":
        return "".join([action, future_spd, sign_type, custom_sign, comments])
    elif action_value == "REMOVE":
        return "".join([action, current_spd, comments])

    raise ValueError(f"Unknown INSTRUCTIONS value for sign feature: {action_value}")


class WorkOrder(object):
    """ An arcgis.Feature wrapper for ETL-ing b/t AGOL and Knack """

    def __init__(self, feature, layers, sr):
        self.geometry = feature.geometry
        self.attributes = feature.attributes
        self.layers = layers
        self.sr = sr
        self.geom_filter = create_geom_filter(self.geometry, self.sr)

    def __repr__(self):
        return str({"attributes": self.attributes, "geometry": self.geometry})

    def pretty(self):
        import pprint

        pprint.pprint({"attributes": self.attributes, "geometry": self.geometry})

    def get_signs(self):
        query_args = {
            "geometry_filter": self.geom_filter,
            # "where": "WORK_ORDER_STATUS = null",
            "out_fields": "*",
            "out_sr": SR_ID,
        }
        self.signs = self.layers.signs.query(**query_args)
        return None

    def identify_eng_area(self, key="ATD_ENGINEER_AREAS"):
        """ identify the engineer service area that intersects with the center of the
        work order """
        wo_geom = arcgis.geometry.Geometry(self.geometry)
        wo_shape = shapely_wkt.loads(wo_geom.WKT)
        wo_centroid = wo_shape.centroid
        wo_centroid_geom = dict(zip(("x", "y"), wo_centroid.coords[0]))
        wo_centroid_filter = create_geom_filter(wo_centroid_geom, self.sr)

        query_args = {
            "geometry_filter": wo_centroid_filter,
            "out_fields": key,
            "out_sr": SR_ID,
        }

        wo_eng_area = self.layers.eng_areas.query(**query_args)

        if not wo_eng_area:
            logging.warning(f"No work area found for {self.attributes['OBJECTID']}")
            self.attributes["WORK_AREA"] = None

        else:
            self.attributes["WORK_AREA"] = wo_eng_area.features[0].attributes[key]

    def identify_street_segments(self):
        # fetch intersecting street segments
        query_args = {
            "geometry_filter": self.geom_filter,
            "out_fields": "*",
            "out_sr": SR_ID,
        }

        self.street_segments = self.layers.street_segments.query(**query_args)

        if not self.street_segments:
            raise ValueError(
                f"No street segments found at work order location: {self.attributes['OBJECTID']}"  # noqa
            )

    def identify_nearest_sign_segments(self):
        """ identify the street segment nearest to each sign. these segments will be
        used to construct the location name """
        self.sign_segments = []
        for sign in self.signs.features:
            nearest_segment, segment_distance = find_nearest_feature(
                sign, self.street_segments
            )
            self.sign_segments.append(nearest_segment)

    def construct_location(self):
        """ Construct a description of the work order location based on the street
        segment names nearest to each sign. It works like this:
        - identify the most common street name among each sign's nearest segment
        - calculate the complete block range across those segments
        - if all street names have equal prevalence, pick the first one in the list

        This handles a common case where a work order is comprised of a series of signs
        running along the same street in a neighborhood. In cases where signs are
        distributed evenly across a neighborhood, the location will merely point one
        to the general area of work. We need feedback from users on other ways to 
        approach this.
        """
        # get all street names and count them
        street_names = [s.attributes["FULL_STREET_NAME"] for s in self.sign_segments]
        name_counts = collections.Counter(street_names)
        # if multiple names occur the same number of times, the earliest is chosen
        most_common_name = max(name_counts.items(), key=operator.itemgetter(1))[0]
        # identify segments with the most common name
        street_segments_block_range = [
            s
            for s in self.sign_segments
            if s.attributes["FULL_STREET_NAME"] == most_common_name
        ]
        # get the total block range of all streets with the most common name
        blk_start, blk_end = block_range(street_segments_block_range)
        # grab one of the segment_ids, which is required by knack for GIS QA
        street_segment_id = street_segments_block_range[0].attributes["SEGMENT_ID"]
        # return formatted location name and reference segment ID
        if len(street_segments_block_range) == 1:
            location_name = f"{blk_start} BLK {most_common_name}"
        else:
            location_name = f"{blk_start}-{blk_end} {most_common_name}"

        self.attributes.update(
            {
                "GENERATED_LOCATION_FROM_API": location_name,
                "LOCATION_STREET_SEGMENT_ID_REFERENCE": street_segment_id,
            }
        )

    def set_knack_user_id(self, user_index):
        """ use the editor email address to lookup the Knack user record ID, which will
        be used to set connections to Knack accounts for created by and modified by """
        editor_email = parse_editor_name(self.attributes["Editor"])
        self.attributes["KNACK_USER_ID"] = user_index.get(editor_email)

    def set_knack_work_order_attributes(self):
        """ Set some default values and do a bit of attribute transforming. Basically
        just smushing a bunch of biz logic in here """
        self.attributes["REQUESTER"] = "TRANSPORTATION ENGINEERING"
        self.attributes["LOCATION_TYPE"] = "Section of Road"
        self.attributes["CREATED_BY_API"] = True
        self.attributes["HOLD_OTHER"] = True
        self.attributes[
            "HOLD_OTHER_REASON"
        ] = "ArcGIS Online auto-update in progress. If you're reading this, something may be broken. Contact transportation.data@austintexas.gov for assistance."  # noqa
        # we wrap these editor connection fields in a list per Knack API requirements
        self.attributes["CREATED_BY"] = [self.attributes["KNACK_USER_ID"]]
        self.attributes["MODIFIED_BY"] = [self.attributes["KNACK_USER_ID"]]

    def create_knack_work_order_payload(self):
        """ replace attribute field names with knack field IDs """
        self.knack_payload = map_fields(self.attributes, FIELD_MAP["work_orders"])

    def create_knack_work_order_update_payload(self):
        """ update the knack work order to set the hold reason to "waiting for digtess """
        hold_reason = "Waiting for DIGTESS"
        knack_hold_reason_field_id = FIELD_MAP["work_orders"]["HOLD_OTHER_REASON"]
        record_id = self.knack_record["id"]
        self.knack_update_payload = {
            "id": record_id,
            knack_hold_reason_field_id: hold_reason,
        }

    def create_knack_signs_payload(self):
        """ Set some default values and do a bit of attribute transforming. Basically
        just smushing a bunch of biz logic in here """
        for sign in self.signs.features:
            sign.attributes["SIGNS_LOCATION"] = sign.geometry
            knack_editor_id = self.attributes["KNACK_USER_ID"]
            sign.attributes["ENGINEER_NOTE"] = construct_engineer_note(sign.attributes)
            sign.attributes["CREATED_BY"] = [knack_editor_id]
            sign.attributes["MODIFIED_BY"] = [knack_editor_id]
            sign.attributes["SIGNS_LOCATION"] = {
                "latitude": sign.attributes["SIGNS_LOCATION"]["y"],
                "longitude": sign.attributes["SIGNS_LOCATION"]["x"],
            }

            sign.knack_payload = map_fields(
                sign.attributes, FIELD_MAP["signs_locations"]
            )

    def create_knack_work_order(self, app):
        try:
            self.knack_record = app.record(
                data=self.knack_payload, method="create", obj="object_176"
            )
        except requests.exceptions.HTTPError as e:
            # make life easier by logging the error message in the response
            logging.error(e.response.text)
            raise e

    def prepare_agol_work_order_edits(self):
        """ Ready the AGOL feature payload with attributes from the Knack work order we
        created """
        self.attributes["KNACK_WORK_ORDER_ID"] = self.knack_record[WORK_ORDER_ID_FIELD]
        self.attributes["KNACK_RECORD_ID"] = self.knack_record["id"]
        self.attributes["WORK_ORDER_STATUS"] = self.knack_record[
            WORK_ORDER_STATUS_FIELD
        ]

    def update_agol_work_order(self):
        """ Update AGOL feature with attributes from the Knack work order we created """
        FIELDS_TO_UPDATE = [
            "OBJECTID",
            "KNACK_WORK_ORDER_ID",
            "KNACK_RECORD_ID",
            "WORK_ORDER_STATUS",
        ]
        attribute_payload = {
            key: val for key, val in self.attributes.items() if key in FIELDS_TO_UPDATE
        }

        feature = {"attributes": attribute_payload, "geometry": self.geometry}
        results = self.layers.work_orders.edit_features(updates=[feature])

        try:
            assert results["updateResults"][0]["success"]
        except (AssertionError, KeyError, TypeError):
            logging.error(results)
            raise ValueError(
                f"AGOL update failed for work order: {self.attributes['KNACK_WORK_ORDER_ID']}"
            )

    def update_sign_attributes_with_knack_record_data(self, sign):
        work_order_id = self.attributes["KNACK_WORK_ORDER_ID"]
        knack_work_order_record_id = self.attributes["KNACK_RECORD_ID"]
        work_order_status = self.attributes["WORK_ORDER_STATUS"]
        knack_location_id = sign.knack_record[SIGN_LOCATION_ID_FIELD]
        knack_location_record_id = sign.knack_record["id"]

        sign.attributes.update(
            {
                "KNACK_WORK_ORDER_ID": work_order_id,
                "KNACK_RECORD_ID": knack_work_order_record_id,
                "WORK_ORDER_STATUS": work_order_status,
                "KNACK_LOCATION_ID": knack_location_id,
                "KNACK_LOCATION_RECORD_ID": knack_location_record_id,
            }
        )

    def update_agol_sign(self, sign):
        """ Update AGOL feature with attributes from the Knack work order we created """
        FIELDS_TO_UPDATE = [
            "OBJECTID",
            "KNACK_WORK_ORDER_ID",
            "KNACK_RECORD_ID",
            "WORK_ORDER_STATUS",
            "KNACK_LOCATION_ID",
            "KNACK_LOCATION_RECORD_ID",
        ]

        attribute_payload = {
            key: val for key, val in sign.attributes.items() if key in FIELDS_TO_UPDATE
        }

        sign.geometry["spatialReference"] = self.signs.spatial_reference

        feature = {"attributes": attribute_payload, "geometry": sign.geometry}

        results = self.layers.signs.edit_features(updates=[feature])

        try:
            assert results["updateResults"][0]["success"]
        except (AssertionError, KeyError, TypeError):
            logging.error(results)
            raise ValueError(
                f"AGOL update failed for location: {attribute_payload['KNACK_LOCATION_ID']}"
            )

    def update_knack_signs_payload(self):
        """ set the parent work order ID for each sign. we wrap this in an array as is
        required by the Knack API """
        work_order_record_id = self.knack_record["id"]

        for sign in self.signs.features:
            sign.knack_payload["field_3298"] = [work_order_record_id]


class Layers(object):
    """ A container for arcgis online layers """

    def __init__(self, auth, agol_services):
        self.gis = arcgis.GIS(**auth)
        self.work_orders_service = self.gis.content.get(
            agol_services["speed_limit_change"]["id"]
        )
        self.work_orders = self.work_orders_service.layers[1]
        self.signs = self.work_orders_service.layers[0]
        self.street_segments_service = self.gis.content.get(
            agol_services["street_segments"]["id"]
        )
        self.street_segments = self.street_segments_service.layers[0]
        self.eng_areas_service = self.gis.content.get(
            agol_services["engineer_areas"]["id"]
        )
        self.eng_areas = self.eng_areas_service.layers[0]


def main():
    LAYERS = Layers(AUTH, AGOL_SERVICES)

    # query work order layer for features to be processed
    wo_feature_set = LAYERS.work_orders.query(
        **{"where": "DESCRIPTION like '%John%'", "out_fields": "*", "out_sr": SR_ID,}
    )

    if not wo_feature_set:
        logging.info("No work order records to process.")
        return

    logging.info(f"{len(wo_feature_set)} work order(s) to process.")

    SR = wo_feature_set.spatial_reference

    # init knack app and fetch accounts. let's get this out of the way early because
    # Knack API is break-y
    app = knackpy.App(app_id=APP_ID, api_key=API_KEY)
    user_index = get_user_index(app, KNACK["accounts"]["id"])

    work_orders = [WorkOrder(feature, LAYERS, SR) for feature in wo_feature_set]

    for wo in work_orders:
        wo.get_signs()
        if not wo.signs:
            # if a work order does not have sign points, it is ignored
            # this should not happen per agreed upon process
            logging.warning(
                f"Work order has no intersecting sign points: {wo.attributes['OBJECTID']}"  # noqa
            )
            continue

        wo.identify_eng_area()
        wo.identify_street_segments()
        wo.identify_nearest_sign_segments()
        wo.construct_location()
        wo.set_knack_user_id(user_index)
        wo.set_knack_work_order_attributes()
        wo.create_knack_work_order_payload()
        wo.create_knack_signs_payload()
        # ------------------------------------------------------------------------------
        # our data is ready to be sent to knack
        # each time we create a record, we update the corresponding feature in AGOL
        # with the record ID. if something goes wrong, we'll be able to pick up where
        # we left off
        # ------------------------------------------------------------------------------
        wo.create_knack_work_order(app)

        logging.info(
            f"Work order {wo.knack_record[WORK_ORDER_ID_FIELD]} created in Knack."
        )

        wo.prepare_agol_work_order_edits()
        wo.update_agol_work_order()

        logging.info(
            f"Work order {wo.knack_record[WORK_ORDER_ID_FIELD]} updated in AGOL."
        )

        wo.update_knack_signs_payload()

        for sign in wo.signs.features:
            sign.knack_record = app.record(
                data=sign.knack_payload, method="create", obj="object_177"
            )
            logging.info(
                f"Location {sign.knack_record[SIGN_LOCATION_ID_FIELD]} updated in Knack."
            )
            wo.update_sign_attributes_with_knack_record_data(sign)
            wo.update_agol_sign(sign)

        # last step: remove or update hold status of work order in knack
        wo.create_knack_work_order_update_payload()
        results = app.record(
            data=wo.knack_update_payload, method="update", obj="object_176"
        )
        breakpoint()
        # ok, now remove work order hold...or switch hold status to whatever that other reason will be


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.project import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response
from gretok.utils.logger import log_info

LOG_TITLE = "Solar Farm Project API"


@frappe.whitelist()
def store_solar_farm_project(**kwargs):
	"""
	API to register a new Solar Farm Project (one-time).

	Endpoint: POST /api/method/gretok.api.v1.solar_farm.project.store_solar_farm_project
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	# Validate
	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	# Check duplicate project name
	if frappe.db.exists("Solar Farm Project", {"project_name": kwargs.get("project_name")}):
		return error_response(
			_("A Solar Farm Project with name '{0}' already exists").format(kwargs.get("project_name")),
			http_status_code=409,
		)

	# Build doc
	doc_data = {"doctype": "Solar Farm Project"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	doc = frappe.get_doc(doc_data)
	doc.insert(ignore_permissions=True)
	frappe.db.commit()

	response_data = _build_project_response(doc)

	frappe.publish_realtime(
		"solar_farm_project_created",
		response_data,
		after_commit=True,
	)

	response = success_response(
		_("Solar Farm Project created successfully"),
		data={"project": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


def _build_project_response(doc):
	return {
		"name": doc.name,
		"project_name": doc.project_name,
		"solar_farm_type": doc.solar_farm_type,
		"commission_date": str(doc.commission_date) if doc.commission_date else None,
		"crediting_period_start_date": str(doc.crediting_period_start_date) if doc.crediting_period_start_date else None,
		"crediting_period_end_date": str(doc.crediting_period_end_date) if doc.crediting_period_end_date else None,
		"applicable_methodology": doc.applicable_methodology,
		"location_state": doc.location_state,
		"location_district": doc.location_district,
		"gps_latitude": doc.gps_latitude,
		"gps_longitude": doc.gps_longitude,
		"dc_installed_capacity_mwp": doc.dc_installed_capacity_mwp,
		"ac_installed_capacity_mw": doc.ac_installed_capacity_mw,
		"panel_technology": doc.panel_technology,
		"inverter_type": doc.inverter_type,
		"grid_connection_type": doc.grid_connection_type,
	}

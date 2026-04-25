import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.project import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Solar Farm Project API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_solar_farm_project(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.solar_farm.project.store_solar_farm_project
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	if frappe.db.exists("Solar Farm Project", {"project_name": kwargs.get("project_name")}):
		return conflict_response(
			_("A Solar Farm Project with name '{0}' already exists").format(kwargs.get("project_name"))
		)

	doc_data = {"doctype": "Solar Farm Project"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to create Solar Farm Project: {0}").format(str(e)), http_status_code=500)

	response_data = _build_project_response(doc)

	frappe.publish_realtime("solar_farm_project_created", response_data, after_commit=True)

	response = success_response(
		_("Solar Farm Project created successfully"),
		data={"project": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_projects(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.solar_farm.project.get_solar_farm_projects

	Query Params:
		limit (int): Default 20, max 100
		offset (int): Default 0
		solar_farm_type (str): Filter by type
		location_state (str): Filter by state
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("partner"):
		filters["partner"] = kwargs.get("partner")
	if kwargs.get("solar_farm_type"):
		filters["solar_farm_type"] = kwargs.get("solar_farm_type")
	if kwargs.get("location_state"):
		filters["location_state"] = kwargs.get("location_state")

	projects = frappe.get_all(
		"Solar Farm Project",
		filters=filters,
		fields=[
			"name", "project_name", "solar_farm_type", "partner",
			"location_state", "location_district",
			"dc_installed_capacity_mwp", "ac_installed_capacity_mw",
			"commission_date", "crediting_period_start_date",
			"crediting_period_end_date", "grid_connection_type",
			"panel_technology", "inverter_type", "creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="creation desc",
	)

	total = frappe.db.count("Solar Farm Project", filters=filters)

	return success_response(
		_("Solar Farm Projects fetched successfully"),
		data={
			"projects": projects,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_project(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.solar_farm.project.get_solar_farm_project?name=SF-PC-001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Solar Farm Project", name):
		return not_found_response(_("Solar Farm Project '{0}' does not exist").format(name))

	doc = frappe.get_doc("Solar Farm Project", name)

	return success_response(
		_("Solar Farm Project fetched successfully"),
		data={"project": _build_full_project_response(doc)},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_solar_farm_project(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.solar_farm.project.update_solar_farm_project

	Body:
		name (str): Mandatory - project ID to update
		... any fields to update
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Solar Farm Project", name):
		return not_found_response(_("Solar Farm Project '{0}' does not exist").format(name))

	# Validate allowed values if select fields are being updated
	if kwargs.get("solar_farm_type") and kwargs["solar_farm_type"] not in ALLOWED_VALUES["solar_farm_type"]:
		return error_response(_("Invalid solar_farm_type value"), http_status_code=400)

	updatable_fields = list(MANDATORY_FIELDS.keys()) + OPTIONAL_FIELDS
	updatable_fields = [f for f in updatable_fields if f != "project_name"]

	try:
		doc = frappe.get_doc("Solar Farm Project", name)
		for field in updatable_fields:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)
		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update Solar Farm Project: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Solar Farm Project updated successfully"),
		data={"project": _build_full_project_response(doc)},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

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


def _build_full_project_response(doc):
	return {
		"name": doc.name,
		"project_name": doc.project_name,
		"partner": doc.partner,
		"solar_farm_type": doc.solar_farm_type,
		"commission_date": str(doc.commission_date) if doc.commission_date else None,
		"crediting_period_start_date": str(doc.crediting_period_start_date) if doc.crediting_period_start_date else None,
		"crediting_period_end_date": str(doc.crediting_period_end_date) if doc.crediting_period_end_date else None,
		"applicable_methodology": doc.applicable_methodology,
		"location_state": doc.location_state,
		"location_district": doc.location_district,
		"gps_latitude": doc.gps_latitude,
		"gps_longitude": doc.gps_longitude,
		"land_use_type_prior": doc.land_use_type_prior,
		"verra_gs_registry_project_id": doc.verra_gs_registry_project_id,
		"cea_registration_number": doc.cea_registration_number,
		"discom_seb_registration_number": doc.discom_seb_registration_number,
		"grid_emission_factor": doc.grid_emission_factor,
		"emission_factor_reference_year": doc.emission_factor_reference_year,
		"dc_installed_capacity_mwp": doc.dc_installed_capacity_mwp,
		"ac_installed_capacity_mw": doc.ac_installed_capacity_mw,
		"panel_technology": doc.panel_technology,
		"panel_wattage_wp": doc.panel_wattage_wp,
		"total_number_of_panels": doc.total_number_of_panels,
		"inverter_type": doc.inverter_type,
		"number_of_inverters": doc.number_of_inverters,
		"design_performance_ratio": doc.design_performance_ratio,
		"design_capacity_utilization_factor": doc.design_capacity_utilization_factor,
		"grid_substation_name": doc.grid_substation_name,
		"grid_connection_voltage_kv": doc.grid_connection_voltage_kv,
		"grid_connection_type": doc.grid_connection_type,
		"offtaker_ppa_counterparty_name": doc.offtaker_ppa_counterparty_name,
		"metering_point_location": doc.metering_point_location,
		"revenue_meter_serial_numbers": doc.revenue_meter_serial_numbers,
		"revenue_meter_accuracy_class": doc.revenue_meter_accuracy_class,
		"revenue_meter_ownership": doc.revenue_meter_ownership,
		"meter_calibration_certificate_date": str(doc.meter_calibration_certificate_date) if doc.meter_calibration_certificate_date else None,
		"auxiliary_meter_serial_number": doc.auxiliary_meter_serial_number,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}
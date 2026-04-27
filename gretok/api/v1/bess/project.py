import frappe
from frappe import _

from gretok.schemas.v1.bess.project import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "BESS Project API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_bess_project(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.project.store_bess_project
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	if kwargs.get("bess_configuration") == "Coupled with Renewable":
		if not kwargs.get("coupled_project_reference"):
			return error_response(
				_("coupled_project_reference is mandatory for Coupled with Renewable configuration"),
				http_status_code=400,
			)
		if not frappe.db.exists("Solar Farm Project", kwargs.get("coupled_project_reference")):
			return not_found_response(
				_("Solar Farm Project '{0}' does not exist").format(kwargs.get("coupled_project_reference"))
			)

	if frappe.db.exists("BESS Project", {"project_name": kwargs.get("project_name")}):
		return conflict_response(
			_("A BESS Project with name '{0}' already exists").format(kwargs.get("project_name"))
		)

	doc_data = {"doctype": "BESS Project"}

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
		return error_response(_("Failed to create BESS Project: {0}").format(str(e)), http_status_code=500)

	response_data = doc.as_dict()

	frappe.publish_realtime("bess_project_created", response_data, after_commit=True)

	response = success_response(
		_("BESS Project created successfully"),
		data={"project": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_bess_projects(**kwargs):
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("partner"):
		filters["partner"] = kwargs.get("partner")
	if kwargs.get("bess_configuration"):
		filters["bess_configuration"] = kwargs.get("bess_configuration")
	if kwargs.get("location_state"):
		filters["location_state"] = kwargs.get("location_state")
	if kwargs.get("battery_technology"):
		filters["battery_technology"] = kwargs.get("battery_technology")

	# ✅ Step 1: get IDs
	names = frappe.get_all(
		"BESS Project",
		filters=filters,
		pluck="name",
		limit=limit,
		start=offset,
		order_by="creation desc"
	)

	# ✅ Step 2: full data build
	full_projects = []
	for name in names:
		doc = frappe.get_doc("BESS Project", name)
		full_projects.append(_build_full_project_response(doc))

	total = frappe.db.count("BESS Project", filters=filters)

	return success_response(
		_("BESS Projects fetched successfully"),
		data={
			"projects": full_projects,   
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_bess_project(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.bess.project.get_bess_project?name=BS-PC-001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("BESS Project", name):
		return not_found_response(_("BESS Project '{0}' does not exist").format(name))

	doc = frappe.get_doc("BESS Project", name)

	return success_response(
		_("BESS Project fetched successfully"),
		data={"project": doc.as_dict()},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_bess_project(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.bess.project.update_bess_project

	Body:
		name (str): Mandatory - project ID to update
		... any fields to update
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("BESS Project", name):
		return not_found_response(_("BESS Project '{0}' does not exist").format(name))

	updatable_fields = [f for f in list(MANDATORY_FIELDS.keys()) + OPTIONAL_FIELDS if f != "project_name"]

	try:
		doc = frappe.get_doc("BESS Project", name)
		for field in updatable_fields:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)
		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update BESS Project: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("BESS Project updated successfully"),
		data={"project": doc.as_dict()},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_project_response(doc):
	return {
		"name": doc.name,
		"project_name": doc.project_name,
		"bess_configuration": doc.bess_configuration,
		"coupled_project_reference": doc.coupled_project_reference,
		"commission_date": str(doc.commission_date) if doc.commission_date else None,
		"crediting_period_start_date": str(doc.crediting_period_start_date) if doc.crediting_period_start_date else None,
		"crediting_period_end_date": str(doc.crediting_period_end_date) if doc.crediting_period_end_date else None,
		"location_state": doc.location_state,
		"location_district": doc.location_district,
		"battery_technology": doc.battery_technology,
		"rated_energy_capacity_kwh": doc.rated_energy_capacity_kwh,
		"rated_power_output_kw": doc.rated_power_output_kw,
		"bess_operating_mode": doc.bess_operating_mode,
		"design_round_trip_efficiency_pct": doc.design_round_trip_efficiency_pct,
	}


def _build_full_project_response(doc):
	return {
		"name": doc.name,
		"project_name": doc.project_name,
		"bess_configuration": doc.bess_configuration,
		"coupled_project_reference": doc.coupled_project_reference,
		"commission_date": str(doc.commission_date) if doc.commission_date else None,
		"crediting_period_start_date": str(doc.crediting_period_start_date) if doc.crediting_period_start_date else None,
		"crediting_period_end_date": str(doc.crediting_period_end_date) if doc.crediting_period_end_date else None,
		"applicable_methodology": doc.applicable_methodology,
		"bess_operating_mode": doc.bess_operating_mode,
		"location_state": doc.location_state,
		"location_district": doc.location_district,
		"gps_latitude": doc.gps_latitude,
		"gps_longitude": doc.gps_longitude,
		"verra_gs_registry_project_id": doc.verra_gs_registry_project_id,
		"cerc_cea_registration_number": doc.cerc_cea_registration_number,
		"discom_interconnection_agreement_no": doc.discom_interconnection_agreement_no,
		"grid_emission_factor": doc.grid_emission_factor,
		"emission_factor_reference_year": doc.emission_factor_reference_year,
		"battery_technology": doc.battery_technology,
		"battery_manufacturer": doc.battery_manufacturer,
		"battery_cell_model": doc.battery_cell_model,
		"rated_energy_capacity_kwh": doc.rated_energy_capacity_kwh,
		"rated_power_output_kw": doc.rated_power_output_kw,
		"c_rate": doc.c_rate,
		"number_of_battery_racks": doc.number_of_battery_racks,
		"design_round_trip_efficiency_pct": doc.design_round_trip_efficiency_pct,
		"design_depth_of_discharge_pct": doc.design_depth_of_discharge_pct,
		"design_soh_at_end_of_life_pct": doc.design_soh_at_end_of_life_pct,
		"battery_thermal_management_type": doc.battery_thermal_management_type,
		"pcs_rated_power_kw": doc.pcs_rated_power_kw,
		"pcs_efficiency_pct": doc.pcs_efficiency_pct,
		"number_of_pcs_units": doc.number_of_pcs_units,
		"ac_connection_voltage_kv": doc.ac_connection_voltage_kv,
		"bms_manufacturer_model": doc.bms_manufacturer_model,
		"ems_scada_system": doc.ems_scada_system,
		"data_logger_sampling_frequency_mins": doc.data_logger_sampling_frequency_mins,
		"grid_substation_name": doc.grid_substation_name,
		"grid_connection_voltage_kv": doc.grid_connection_voltage_kv,
		"revenue_meter_serial_number": doc.revenue_meter_serial_number,
		"revenue_meter_accuracy_class": doc.revenue_meter_accuracy_class,
		"revenue_meter_ownership": doc.revenue_meter_ownership,
		"revenue_meter_calibration_date": str(doc.revenue_meter_calibration_date) if doc.revenue_meter_calibration_date else None,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}
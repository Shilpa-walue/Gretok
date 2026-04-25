import frappe
from frappe import _

from gretok.schemas.v1.bess.monthly_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "BESS Monthly Data API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_bess_monthly_data(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.monthly_data.store_bess_monthly_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	project = kwargs.get("project")
	if not frappe.db.exists("BESS Project", project):
		return not_found_response(_("BESS Project '{0}' does not exist").format(project))

	if frappe.db.exists("BESS Monthly Data", {
		"project": project,
		"reporting_month": kwargs.get("reporting_month"),
	}):
		return conflict_response(
			_("Monthly data for project '{0}' and month '{1}' already exists").format(
				project, kwargs.get("reporting_month")
			)
		)

	doc_data = {"doctype": "BESS Monthly Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	energy_charged_solar = float(kwargs.get("total_energy_charged_from_solar_kwh") or 0)
	energy_charged_grid = float(kwargs.get("total_energy_charged_from_grid_kwh") or 0)
	energy_discharged = float(kwargs.get("total_energy_discharged_to_grid_kwh") or 0)
	total_charged = energy_charged_solar + energy_charged_grid

	if total_charged > 0:
		doc_data["actual_round_trip_efficiency_pct"] = round((energy_discharged / total_charged) * 100, 2)

	project_doc = frappe.get_doc("BESS Project", project)
	rated_capacity = project_doc.rated_energy_capacity_kwh or 0
	if rated_capacity > 0:
		doc_data["number_of_full_equivalent_cycles"] = round(energy_discharged / rated_capacity, 2)

	planned = float(kwargs.get("planned_downtime_hours") or 0)
	unplanned = float(kwargs.get("unplanned_downtime_hours") or 0)
	total_hours = 30 * 24
	available_hours = total_hours - planned - unplanned
	doc_data["system_availability_pct"] = round((available_hours / total_hours) * 100, 2)

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to store BESS Monthly Data: {0}").format(str(e)), http_status_code=500)

	response_data = _build_monthly_response(doc)

	frappe.publish_realtime("bess_monthly_data_stored", response_data, after_commit=True)

	response = success_response(
		_("BESS Monthly Data stored successfully"),
		data={"monthly_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_bess_monthly_data_list(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.bess.monthly_data.get_bess_monthly_data_list

	Query Params:
		project (str): Filter by project ID
		limit (int): Default 20
		offset (int): Default 0
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("project"):
		filters["project"] = kwargs.get("project")

	records = frappe.get_all(
		"BESS Monthly Data",
		filters=filters,
		fields=[
			"name", "project", "reporting_month",
			"total_energy_discharged_to_grid_kwh",
			"actual_round_trip_efficiency_pct",
			"number_of_full_equivalent_cycles",
			"average_state_of_health_pct",
			"system_availability_pct",
			"creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="reporting_month desc",
	)

	total = frappe.db.count("BESS Monthly Data", filters=filters)

	return success_response(
		_("BESS Monthly Data fetched successfully"),
		data={
			"records": records,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_bess_monthly_data(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.bess.monthly_data.get_bess_monthly_data?name=BESSMD-001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("BESS Monthly Data", name):
		return not_found_response(_("BESS Monthly Data '{0}' does not exist").format(name))

	doc = frappe.get_doc("BESS Monthly Data", name)

	return success_response(
		_("BESS Monthly Data fetched successfully"),
		data={"monthly_data": _build_monthly_response(doc)},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_bess_monthly_data(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.bess.monthly_data.update_bess_monthly_data

	Body:
		name (str): Mandatory - record ID to update
		... any fields to update
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("BESS Monthly Data", name):
		return not_found_response(_("BESS Monthly Data '{0}' does not exist").format(name))

	try:
		doc = frappe.get_doc("BESS Monthly Data", name)

		for field in OPTIONAL_FIELDS:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)

		# Recalculate derived fields
		energy_charged_solar = float(doc.total_energy_charged_from_solar_kwh or 0)
		energy_charged_grid = float(doc.total_energy_charged_from_grid_kwh or 0)
		energy_discharged = float(doc.total_energy_discharged_to_grid_kwh or 0)
		total_charged = energy_charged_solar + energy_charged_grid

		if total_charged > 0:
			doc.actual_round_trip_efficiency_pct = round((energy_discharged / total_charged) * 100, 2)

		project_doc = frappe.get_doc("BESS Project", doc.project)
		rated_capacity = project_doc.rated_energy_capacity_kwh or 0
		if rated_capacity > 0:
			doc.number_of_full_equivalent_cycles = round(energy_discharged / rated_capacity, 2)

		planned = float(doc.planned_downtime_hours or 0)
		unplanned = float(doc.unplanned_downtime_hours or 0)
		total_hours = 30 * 24
		available_hours = total_hours - planned - unplanned
		doc.system_availability_pct = round((available_hours / total_hours) * 100, 2)

		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update BESS Monthly Data: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("BESS Monthly Data updated successfully"),
		data={"monthly_data": _build_monthly_response(doc)},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_monthly_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"reporting_month": str(doc.reporting_month) if doc.reporting_month else None,
		"total_energy_charged_from_solar_kwh": doc.total_energy_charged_from_solar_kwh,
		"total_energy_charged_from_grid_kwh": doc.total_energy_charged_from_grid_kwh,
		"total_energy_discharged_to_grid_kwh": doc.total_energy_discharged_to_grid_kwh,
		"auxiliary_consumption_kwh": doc.auxiliary_consumption_kwh,
		"actual_round_trip_efficiency_pct": doc.actual_round_trip_efficiency_pct,
		"number_of_full_equivalent_cycles": doc.number_of_full_equivalent_cycles,
		"average_state_of_health_pct": doc.average_state_of_health_pct,
		"system_availability_pct": doc.system_availability_pct,
		"planned_downtime_hours": doc.planned_downtime_hours,
		"unplanned_downtime_hours": doc.unplanned_downtime_hours,
		"bms_alarm_events_count": doc.bms_alarm_events_count,
		"bms_alarm_types": doc.bms_alarm_types,
		"curtailment_duration_hours": doc.curtailment_duration_hours,
		"curtailment_energy_lost_kwh": doc.curtailment_energy_lost_kwh,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}
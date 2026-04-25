import json
import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.rt_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES, DEFAULTS
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Solar Farm RT Data API"


# ── CREATE SINGLE ─────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_solar_farm_rt_data(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.solar_farm.rt_data.store_solar_farm_rt_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	project = kwargs.get("project")
	if not frappe.db.exists("Solar Farm Project", project):
		return not_found_response(_("Solar Farm Project '{0}' does not exist").format(project))

	doc_data = {"doctype": "Solar Farm RT Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value
		elif field in DEFAULTS:
			doc_data[field] = DEFAULTS[field]

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to store Solar Farm RT Data: {0}").format(str(e)), http_status_code=500)

	response_data = _build_rt_response(doc)

	response = success_response(
		_("Solar Farm RT Data stored successfully"),
		data={"rt_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── CREATE BATCH ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_solar_farm_rt_data_batch(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.solar_farm.rt_data.store_solar_farm_rt_data_batch
	"""
	kwargs.pop("cmd", None)

	records = kwargs.get("records") or []
	if isinstance(records, str):
		records = json.loads(records)

	if not records:
		return error_response(_("No records provided"), http_status_code=400)

	log_info(LOG_TITLE, "Batch Incoming", {"count": len(records)})

	project = records[0].get("project") if records else None
	if project and not frappe.db.exists("Solar Farm Project", project):
		return not_found_response(_("Solar Farm Project '{0}' does not exist").format(project))

	created = []
	errors = []

	for idx, record in enumerate(records):
		try:
			error = validate_payload(record, MANDATORY_FIELDS, ALLOWED_VALUES)
			if error:
				errors.append({"index": idx, "timestamp": record.get("timestamp"), "error": error})
				continue

			doc_data = {"doctype": "Solar Farm RT Data"}

			for field in MANDATORY_FIELDS:
				doc_data[field] = record.get(field)

			for field in OPTIONAL_FIELDS:
				value = record.get(field)
				if value is not None:
					doc_data[field] = value
				elif field in DEFAULTS:
					doc_data[field] = DEFAULTS[field]

			doc = frappe.get_doc(doc_data)
			doc.insert(ignore_permissions=True)
			created.append(doc.name)

		except Exception as e:
			log_error(LOG_TITLE, f"Batch Insert Failed at index {idx}", record, exc=e)
			errors.append({"index": idx, "timestamp": record.get("timestamp"), "error": str(e)})

	frappe.db.commit()

	response = success_response(
		_("{0} records created, {1} failed").format(len(created), len(errors)),
		data={
			"created_count": len(created),
			"error_count": len(errors),
			"created": created,
			"errors": errors,
		},
	)

	log_info(LOG_TITLE, "Batch Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_rt_data_list(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.solar_farm.rt_data.get_solar_farm_rt_data_list

	Query Params:
		project (str): Filter by project ID
		from_date (str): Filter from datetime e.g. 2024-03-15 08:00:00
		to_date (str): Filter to datetime
		limit (int): Default 20
		offset (int): Default 0
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 500)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("project"):
		filters["project"] = kwargs.get("project")
	if kwargs.get("from_date"):
		filters["timestamp"] = [">=", kwargs.get("from_date")]
	if kwargs.get("to_date"):
		filters["timestamp"] = ["<=", kwargs.get("to_date")]

	records = frappe.get_all(
		"Solar Farm RT Data",
		filters=filters,
		fields=[
			"name", "project", "timestamp", "inverter_status",
			"active_power_generation_kw", "energy_generated_interval_kwh",
			"active_power_export_kw", "solar_irradiance_ghi_wm2",
			"ambient_temperature_c", "grid_availability_flag",
		],
		limit=limit,
		start=offset,
		order_by="timestamp desc",
	)

	total = frappe.db.count("Solar Farm RT Data", filters=filters)

	return success_response(
		_("Solar Farm RT Data fetched successfully"),
		data={
			"records": records,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_rt_data(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.solar_farm.rt_data.get_solar_farm_rt_data?name=SFRT-2024-03-15-001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Solar Farm RT Data", name):
		return not_found_response(_("Solar Farm RT Data '{0}' does not exist").format(name))

	doc = frappe.get_doc("Solar Farm RT Data", name)

	return success_response(
		_("Solar Farm RT Data fetched successfully"),
		data={"rt_data": _build_rt_response(doc)},
	)


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_rt_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"timestamp": str(doc.timestamp) if doc.timestamp else None,
		"inverter_status": doc.inverter_status,
		"grid_availability_flag": doc.grid_availability_flag,
		"active_power_generation_kw": doc.active_power_generation_kw,
		"energy_generated_interval_kwh": doc.energy_generated_interval_kwh,
		"active_power_export_kw": doc.active_power_export_kw,
		"grid_frequency_hz": doc.grid_frequency_hz,
		"solar_irradiance_ghi_wm2": doc.solar_irradiance_ghi_wm2,
		"module_temperature_c": doc.module_temperature_c,
		"ambient_temperature_c": doc.ambient_temperature_c,
		"creation": str(doc.creation),
	}
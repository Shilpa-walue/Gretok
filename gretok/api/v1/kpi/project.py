import frappe
from frappe import _

from gretok.schemas.v1.kpi.project import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "KPI Group Project API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_kpi_project(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.kpi.project.store_kpi_project
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	# Check duplicate — same partner + same reporting year
	if frappe.db.exists("KPI Group Project", {
		"partner": kwargs.get("partner"),
		"reporting_year": kwargs.get("reporting_year"),
	}):
		return conflict_response(
			_("A KPI Group Project already exists for partner '{0}' and year '{1}'").format(
				kwargs.get("partner"), kwargs.get("reporting_year")
			)
		)

	# Validate previous_year_report exists if provided
	if kwargs.get("previous_year_report"):
		if not frappe.db.exists("KPI Group Project", kwargs.get("previous_year_report")):
			return not_found_response(
				_("Previous Year Report '{0}' does not exist").format(kwargs.get("previous_year_report"))
			)

	# Validate partner exists
	if not frappe.db.exists("Partners", kwargs.get("partner")):
		return not_found_response(
			_("Partner '{0}' does not exist").format(kwargs.get("partner"))
		)

	doc_data = {
		"doctype": "KPI Group Project",
		"naming_series": "KPIP-.####",
	}

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
		return error_response(_("Failed to create KPI Group Project: {0}").format(str(e)), http_status_code=500)

	response_data = _build_full_response(doc)

	frappe.publish_realtime("kpi_project_created", response_data, after_commit=True)

	response = success_response(
		_("KPI Group Project created successfully"),
		data={"kpi_project": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_kpi_projects(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.kpi.project.get_kpi_projects

	Query Params:
		partner (str): Filter by partner ID
		reporting_year (str): Filter by year e.g. FY2025
		limit (int): Default 20
		offset (int): Default 0
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("partner"):
		filters["partner"] = kwargs.get("partner")
	if kwargs.get("reporting_year"):
		filters["reporting_year"] = kwargs.get("reporting_year")

	projects = frappe.get_all(
		"KPI Group Project",
		filters=filters,
		fields=[
			"name", "partner", "organisation_name",
			"reporting_year", "base_year", "previous_year_report",
			"scope_1_tco2e", "scope_2_tco2e", "scope_3_tco2e",
			"total_gross_emissions_tco2e", "total_net_emissions_tco2e",
			"current_year_gross_intensity", "creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="reporting_year desc",
	)

	total = frappe.db.count("KPI Group Project", filters=filters)

	return success_response(
		_("KPI Group Projects fetched successfully"),
		data={
			"projects": projects,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_kpi_project(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.kpi.project.get_kpi_project?name=KPIP-0001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("KPI Group Project", name):
		return not_found_response(_("KPI Group Project '{0}' does not exist").format(name))

	doc = frappe.get_doc("KPI Group Project", name)

	# Fetch linked category summary count
	category_count = frappe.db.count("KPI Category Summary", {"kpi_project": name})

	# Fetch linked activity summary count
	activity_count = frappe.db.count("KPI Activity Summary", {"kpi_project": name})

	response_data = _build_full_response(doc)
	response_data["category_summary_count"] = category_count
	response_data["activity_summary_count"] = activity_count

	return success_response(
		_("KPI Group Project fetched successfully"),
		data={"kpi_project": response_data},
	)


# ── FETCH SINGLE WITH FULL DATA ───────────────────────────────────────────────

@frappe.whitelist()
def get_kpi_project_full(**kwargs):
	"""
	Fetch KPI Group Project with all Category and Activity Summary data included.

	Endpoint: GET /api/method/gretok.api.v1.kpi.project.get_kpi_project_full?name=KPIP-0001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("KPI Group Project", name):
		return not_found_response(_("KPI Group Project '{0}' does not exist").format(name))

	doc = frappe.get_doc("KPI Group Project", name)

	# Fetch all category summary records
	categories = frappe.get_all(
		"KPI Category Summary",
		filters={"kpi_project": name},
		fields=[
			"name", "category",
			"base_year_emissions_tco2e",
			"previous_year_gross_emissions_tco2e",
			"previous_year_net_emissions_tco2e",
			"current_year_gross_emissions_tco2e",
			"contribution_to_total_pct",
			"scope_1_tco2e", "scope_2_tco2e", "scope_3_tco2e",
			"ghg_trades_tco2e",
			"current_year_net_emissions_tco2e",
			"gross_change_on_previous_year",
			"pct_gross_change_on_previous_year",
			"net_change_on_previous_year",
			"pct_net_change_on_previous_year",
			"change_on_base_year",
			"pct_change_on_base_year",
			"comments",
		],
		order_by="category asc",
	)

	# Fetch all activity summary records
	activities = frappe.get_all(
		"KPI Activity Summary",
		filters={"kpi_project": name},
		fields=[
			"name", "category", "activity", "unit",
			"base_year_activity",
			"previous_year_activity",
			"current_year_activity",
			"base_year_emissions_tco2e",
			"previous_year_gross_emissions_tco2e",
			"current_year_gross_emissions_tco2e",
			"pct_of_total_emissions",
			"scope_1_tco2e", "scope_2_tco2e", "scope_3_tco2e",
			"current_year_net_emissions_tco2e",
			"gross_change_on_previous_year_emissions",
			"pct_gross_change_on_previous_year_emissions",
			"change_on_base_year_emissions",
			"pct_change_on_base_year_emissions",
			"comments",
		],
		order_by="category asc, activity asc",
	)

	response_data = _build_full_response(doc)
	response_data["category_summary"] = categories
	response_data["activity_summary"] = activities

	return success_response(
		_("KPI Group Project fetched successfully with full data"),
		data={"kpi_project": response_data},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_kpi_project(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.kpi.project.update_kpi_project

	Body:
		name (str): Mandatory - KPI Project ID
		... any fields to update
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("KPI Group Project", name):
		return not_found_response(_("KPI Group Project '{0}' does not exist").format(name))

	try:
		doc = frappe.get_doc("KPI Group Project", name)
		for field in OPTIONAL_FIELDS:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)
		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update KPI Group Project: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("KPI Group Project updated successfully"),
		data={"kpi_project": _build_full_response(doc)},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_full_response(doc):
	return {
		"name": doc.name,
		"partner": doc.partner,
		"organisation_name": doc.organisation_name,
		"reporting_year": doc.reporting_year,
		"base_year": doc.base_year,
		"previous_year_report": doc.previous_year_report,
		"scope_1_tco2e": doc.scope_1_tco2e,
		"scope_2_tco2e": doc.scope_2_tco2e,
		"scope_3_tco2e": doc.scope_3_tco2e,
		"total_gross_emissions_tco2e": doc.total_gross_emissions_tco2e,
		"ghg_trades_tco2e": doc.ghg_trades_tco2e,
		"total_net_emissions_tco2e": doc.total_net_emissions_tco2e,
		"scope3_cat1_purchased_goods_tco2e": doc.scope3_cat1_purchased_goods_tco2e,
		"scope3_cat2_capital_goods_tco2e": doc.scope3_cat2_capital_goods_tco2e,
		"scope3_cat3_fuel_energy_tco2e": doc.scope3_cat3_fuel_energy_tco2e,
		"scope3_cat4_upstream_transport_tco2e": doc.scope3_cat4_upstream_transport_tco2e,
		"scope3_cat5_waste_tco2e": doc.scope3_cat5_waste_tco2e,
		"scope3_cat6_business_travel_tco2e": doc.scope3_cat6_business_travel_tco2e,
		"scope3_cat7_employee_commute_tco2e": doc.scope3_cat7_employee_commute_tco2e,
		"scope3_cat8_upstream_leased_tco2e": doc.scope3_cat8_upstream_leased_tco2e,
		"scope3_cat9_downstream_transport_tco2e": doc.scope3_cat9_downstream_transport_tco2e,
		"scope3_cat10_processing_sold_tco2e": doc.scope3_cat10_processing_sold_tco2e,
		"scope3_cat11_use_sold_tco2e": doc.scope3_cat11_use_sold_tco2e,
		"scope3_cat12_end_of_life_tco2e": doc.scope3_cat12_end_of_life_tco2e,
		"scope3_cat13_downstream_leased_tco2e": doc.scope3_cat13_downstream_leased_tco2e,
		"scope3_cat14_franchises_tco2e": doc.scope3_cat14_franchises_tco2e,
		"scope3_cat15_investments_tco2e": doc.scope3_cat15_investments_tco2e,
		"intensity_metric": doc.intensity_metric,
		"intensity_unit": doc.intensity_unit,
		"base_year_intensity": doc.base_year_intensity,
		"previous_year_gross_intensity": doc.previous_year_gross_intensity,
		"previous_year_net_intensity": doc.previous_year_net_intensity,
		"current_year_gross_intensity": doc.current_year_gross_intensity,
		"current_year_net_intensity": doc.current_year_net_intensity,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}
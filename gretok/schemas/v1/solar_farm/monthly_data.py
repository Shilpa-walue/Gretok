MANDATORY_FIELDS = {
	"project": str,
	"reporting_month": str,
	"opening_meter_reading_kwh": float,
	"opening_reading_datetime": str,
	"closing_meter_reading_kwh": float,
	"closing_reading_datetime": str,
}

OPTIONAL_FIELDS = [
	"utility_representative_name",
	"company_representative_name",
	"meter_photo_evidence",
	"auxiliary_consumption_kwh",
	"grid_curtailment_energy_lost_kwh",
	"plant_operational_days",
	"planned_downtime_hours",
	"unplanned_downtime_hours",
]

ALLOWED_VALUES = {}

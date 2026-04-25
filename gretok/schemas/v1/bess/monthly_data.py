MANDATORY_FIELDS = {
	"project": str,
	"reporting_month": str,
	"total_energy_discharged_to_grid_kwh": float,
}

OPTIONAL_FIELDS = [
	"total_energy_charged_from_solar_kwh",
	"total_energy_charged_from_grid_kwh",
	"auxiliary_consumption_kwh",
	"average_state_of_health_pct",
	"planned_downtime_hours",
	"unplanned_downtime_hours",
	"bms_alarm_events_count",
	"bms_alarm_types",
	"curtailment_duration_hours",
	"curtailment_energy_lost_kwh",
]

ALLOWED_VALUES = {}
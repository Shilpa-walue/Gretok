MANDATORY_FIELDS = {
	"project": str,
	"timestamp": str,
	"system_operating_mode": str,
	"pcs_inverter_status": str,
	"battery_soc_pct": float,
	"battery_rated_capacity_kwh": float,
	"grid_export_energy_kwh": float,
	"active_power_export_kw": float,
	"ambient_temperature_c": float,
}

OPTIONAL_FIELDS = [
	"system_availability_flag",
	"battery_soh_pct",
	"battery_cycle_count",
	"battery_charge_power_kw",
	"battery_discharge_power_kw",
	"round_trip_efficiency_interval_pct",
	"grid_import_energy_kwh",
	"solar_generation_input_kw",
	"auxiliary_consumption_kw",
	"battery_cell_temp_avg_c",
	"battery_cell_temp_max_c",
	"solar_irradiance_ghi_wm2",
	"bms_alarm_flag",
]

ALLOWED_VALUES = {
	"system_operating_mode": ["CHARGE", "DISCHARGE", "IDLE", "FAULT"],
	"pcs_inverter_status": ["ONLINE", "OFFLINE", "FAULT"],
}

DEFAULTS = {
	"system_availability_flag": 1,
	"bms_alarm_flag": 0,
	"battery_cycle_count": 0,
}
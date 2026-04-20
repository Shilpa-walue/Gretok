MANDATORY_FIELDS = {
	"project": str,
	"timestamp": str,
	"inverter_status": str,
	"active_power_generation_kw": float,
	"energy_generated_interval_kwh": float,
	"active_power_export_kw": float,
	"solar_irradiance_ghi_wm2": float,
	"ambient_temperature_c": float,
}

OPTIONAL_FIELDS = [
	"grid_availability_flag",
	"grid_frequency_hz",
	"module_temperature_c",
]

ALLOWED_VALUES = {
	"inverter_status": ["RUNNING", "STANDBY", "FAULT", "OFFLINE"],
}

DEFAULTS = {
	"grid_availability_flag": 1,
	"grid_frequency_hz": 50.0,
}

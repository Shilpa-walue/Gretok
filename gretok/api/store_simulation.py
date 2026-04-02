import frappe
from frappe import _

@frappe.whitelist(allow_guest=False)
def store_simulation_data(data):
    """
    POST /api/method/<your_app>.api.store_simulation_data
    Body: { "data": { ...fields... } }
    """
    if isinstance(data, str):
        import json
        data = json.loads(data)

    doc = frappe.get_doc({
        "doctype": "Real Data Simulation",
        "naming_series": "RDS-.YYYY.-.MM.-.DD.-.###",

        # System
        "timestamp":                data.get("timestamp"),
        "system_mode":              data.get("system_mode"),
        "inverter_status":          data.get("inverter_status"),

        # Power & Energy
        "solar_generation_kw":      data.get("solar_generation_kw"),
        "battery_charge_kw":        data.get("battery_charge_kw"),
        "battery_discharge_kw":     data.get("battery_discharge_kw"),
        "active_power_export_kw":   data.get("active_power_export_kw"),
        "auxiliary_consumption_kw": data.get("auxiliary_consumption_kw"),
        "grid_export_kwh":          data.get("grid_export_kwh"),
        "grid_import_kwh":          data.get("grid_import_kwh"),
        "round_trip_efficiency_pct":data.get("round_trip_efficiency_pct", 90),
        "system_availability_pct":  data.get("system_availability_pct"),

        # Battery State
        "battery_soc_pct":              data.get("battery_soc_pct"),
        "battery_rated_capacity_kwh":   data.get("battery_rated_capacity_kwh"),
        "battery_available_capacity_kwh": data.get("battery_available_capacity_kwh"),
        "depth_of_discharge_pct":       data.get("depth_of_discharge_pct"),
        "battery_soh_pct":              data.get("battery_soh_pct"),
        "battery_cycle_count":          data.get("battery_cycle_count", 0),
        "bms_alarm_flag":               data.get("bms_alarm_flag", 0),

        # Environmental & Thermal
        "solar_irradiance_wm2":  data.get("solar_irradiance_wm2"),
        "ambient_temp_c":        data.get("ambient_temp_c"),
        "battery_cell_temp_c":   data.get("battery_cell_temp_c"),
        "wind_speed_ms":         data.get("wind_speed_ms"),
        "inverter_temp_c":       data.get("inverter_temp_c"),

        # Grid Interface
        "power_factor":        data.get("power_factor", 0.97),
        "grid_voltage_v":      data.get("grid_voltage_v", 11000),
        "grid_frequency_hz":   data.get("grid_frequency_hz", 50),
        "reactive_power_kvar": data.get("reactive_power_kvar"),
    })

    doc.insert(ignore_permissions=True)
    frappe.db.commit()

    return {
        "success": True,
        "name": doc.name,
        "message": f"Simulation record {doc.name} created successfully"
    }

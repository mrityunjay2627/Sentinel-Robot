"""The data contract — expanded for realistic breadth and preprocessing depth.

Entity design (normalised, OLTP-shaped):
  robot_models            dimension — 6 models with detailed specs
  zones                   dimension — 10 warehouse zones
  robots                  one row per robot (registration / current state)
  robot_attribute_changes change-log feeding the SCD2 robot dimension
  sensor_readings         high-volume fact; 30+ sensor fields per reading
                          NESTED payload (STRUCT/ARRAY) for semi-structured work
  tasks                   operational events with lifecycle timestamps
  maintenance_tickets     accumulating-snapshot lifecycle + the ML failure label
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


# --------------------------------------------------------------------------- #
# Categorical domains
# --------------------------------------------------------------------------- #
class ZoneType(str, Enum):
    STORAGE = "STORAGE"
    PICKING = "PICKING"
    PACKING = "PACKING"
    CHARGING = "CHARGING"
    TRANSIT = "TRANSIT"
    STAGING = "STAGING"
    SHIPPING = "SHIPPING"
    RECEIVING = "RECEIVING"


class TaskType(str, Enum):
    PICK = "PICK"
    PUT = "PUT"
    MOVE = "MOVE"
    INSPECT = "INSPECT"
    CHARGE = "CHARGE"
    IDLE = "IDLE"


class TaskStatus(str, Enum):
    ASSIGNED = "ASSIGNED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class FailureType(str, Enum):
    BATTERY = "BATTERY"
    MOTOR = "MOTOR"
    SENSOR = "SENSOR"
    WHEEL = "WHEEL"
    SOFTWARE = "SOFTWARE"
    NAVIGATION = "NAVIGATION"
    COMMUNICATION = "COMMUNICATION"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


FIRMWARE_VERSIONS = ["2.8.1", "3.1.0", "3.2.0", "3.4.1", "4.0.0", "4.1.2", "4.2.0"]
ERROR_CODE_POOL = [
    "E101", "E102", "E205", "E206", "E310", "E311",
    "W402", "W403", "E520", "E521", "W118", "W119",
    "E777", "E888", "E999", "W001", "W055", "E450",
]


# --------------------------------------------------------------------------- #
# Nested payload — the STRUCT/ARRAY that exercises semi-structured work.
# Expanded to 30+ fields across 6 sub-structs.
# --------------------------------------------------------------------------- #
@dataclass
class Battery:
    voltage_v: float
    soc_pct: float
    temp_c: float
    charge_cycles: int
    current_amps: float
    health_pct: float         # battery degradation 0-100

@dataclass
class Motor:
    temp_c: float
    rpm: float
    load_pct: float
    current_amps: float
    vibration_g: float        # accelerometer on motor housing

@dataclass
class Navigation:
    x_m: float
    y_m: float
    heading_deg: float        # 0-360
    speed_mps: float
    zone_id: str
    lidar_front_m: float
    lidar_rear_m: float
    lidar_left_m: float
    lidar_right_m: float
    obstacle_detected: bool

@dataclass
class Environment:
    ambient_temp_c: float
    humidity_pct: float
    light_level_lux: float
    floor_condition: str      # 'DRY', 'WET', 'DUSTY', 'DEBRIS'

@dataclass
class Connectivity:
    wifi_signal_dbm: float
    latency_ms: float
    packet_loss_pct: float
    access_point_id: str

@dataclass
class WheelEncoder:
    left_rpm: float
    right_rpm: float
    odometer_m: float


# --------------------------------------------------------------------------- #
# Entities
# --------------------------------------------------------------------------- #
@dataclass
class RobotModel:
    model_id: str
    model_name: str
    manufacturer: str
    max_payload_kg: float
    battery_capacity_wh: float
    weight_kg: float
    max_speed_mps: float
    sensor_suite: str         # 'BASIC', 'STANDARD', 'ADVANCED'
    wear_rate: float

@dataclass
class Zone:
    zone_id: str
    zone_name: str
    site: str
    zone_type: ZoneType
    floor_area_sqm: float
    max_robots: int
    has_charging: bool

@dataclass
class Robot:
    robot_id: str
    serial_number: str
    model_id: str
    commissioned_date: datetime
    home_zone_id: str
    initial_firmware: str
    owner_team: str
    ip_address: str
    mac_address: str

@dataclass
class AttributeChange:
    change_id: str
    robot_id: str
    changed_at: datetime
    attribute: str
    old_value: str
    new_value: str

@dataclass
class SensorReading:
    reading_id: str
    robot_id: str
    reading_ts: datetime
    battery: Battery
    motor: Motor
    navigation: Navigation
    environment: Environment
    connectivity: Connectivity
    wheels: WheelEncoder
    error_codes: list[str]
    power_draw_watts: float
    operational_status: str   # 'ACTIVE', 'IDLE', 'CHARGING', 'MAINTENANCE'

@dataclass
class Task:
    task_id: str
    robot_id: str
    task_type: TaskType
    status: TaskStatus
    priority: int             # 1-5
    assigned_ts: datetime
    started_ts: Optional[datetime]
    completed_ts: Optional[datetime]
    distance_m: Optional[float]
    payload_kg: Optional[float]
    source_zone_id: Optional[str]
    destination_zone_id: Optional[str]
    error_count: int

@dataclass
class MaintenanceTicket:
    ticket_id: str
    robot_id: str
    failure_type: FailureType
    severity: Severity
    reported_ts: datetime
    diagnosed_ts: Optional[datetime]
    parts_ordered_ts: Optional[datetime]
    repaired_ts: Optional[datetime]
    closed_ts: Optional[datetime]
    root_cause: Optional[str]
    cost_usd: Optional[float]
    downtime_hours: Optional[float]


# --------------------------------------------------------------------------- #
# Table metadata for validation
# --------------------------------------------------------------------------- #
TABLES = [
    "robot_models", "zones", "robots", "robot_attribute_changes",
    "sensor_readings", "tasks", "maintenance_tickets",
]

FOREIGN_KEYS = {
    ("robots", "model_id"): ("robot_models", "model_id"),
    ("robots", "home_zone_id"): ("zones", "zone_id"),
    ("robot_attribute_changes", "robot_id"): ("robots", "robot_id"),
    ("sensor_readings", "robot_id"): ("robots", "robot_id"),
    ("tasks", "robot_id"): ("robots", "robot_id"),
    ("maintenance_tickets", "robot_id"): ("robots", "robot_id"),
}

PRIMARY_KEYS = {
    "robot_models": "model_id",
    "zones": "zone_id",
    "robots": "robot_id",
    "robot_attribute_changes": "change_id",
    "sensor_readings": "reading_id",
    "tasks": "task_id",
    "maintenance_tickets": "ticket_id",
}

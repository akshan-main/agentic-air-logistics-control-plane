# Signals module - derived signals from raw evidence
from .derive import SignalDeriver, DerivedSignal, derive_signals_for_airport
from .congestion import derive_congestion_signal
from .weather_risk import derive_weather_signal
from .movement_collapse import derive_movement_collapse_signal
from .contradiction import detect_contradictions, ContradictionResult

__all__ = [
    "SignalDeriver",
    "DerivedSignal",
    "derive_signals_for_airport",
    "derive_congestion_signal",
    "derive_weather_signal",
    "derive_movement_collapse_signal",
    "detect_contradictions",
    "ContradictionResult",
]

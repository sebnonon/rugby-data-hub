"""
Parsers STATSports — Rugby Data Hub
Un module par type de fichier CSV source.
"""

from . import gps_match, gps_entrainement, gps_collision, gps_melee, actions_match

# Mapping type de fichier → module parser
PARSERS = {
    "gps_match":        gps_match,
    "gps_entrainement": gps_entrainement,
    "gps_collision":    gps_collision,
    "gps_melee":        gps_melee,
    "actions_match":    actions_match,
}

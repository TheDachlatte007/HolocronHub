"""Normalize community mission drop tables without estimating mission duration."""
import math


def flatten_mission_rewards(payload: dict) -> list[dict]:
    planets = payload.get("missionRewards") if isinstance(payload, dict) else None
    if not isinstance(planets, dict) or not planets:
        raise ValueError("Invalid mission drop dataset")
    result = []
    for planet, nodes in planets.items():
        if not isinstance(nodes, dict):
            continue
        for node, mission in nodes.items():
            if not isinstance(mission, dict):
                continue
            rewards = mission.get("rewards") or {}
            rotations = rewards if isinstance(rewards, dict) else {"Single reward": rewards}
            for rotation, entries in rotations.items():
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict) or not entry.get("itemName"):
                        continue
                    try:
                        chance = float(entry.get("chance"))
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(chance) or not 0 < chance <= 100:
                        continue
                    result.append({"planet": str(planet), "node": str(node),
                        "game_mode": str(mission.get("gameMode") or "Unknown"),
                        "is_event": mission.get("isEvent") is True,
                        "rotation": str(rotation), "item_name": str(entry["itemName"]),
                        "chance": chance, "rarity": str(entry.get("rarity") or ""),
                        "expected_reward_checks": round(100 / chance, 2)})
    if not result:
        raise ValueError("No valid mission drop rows")
    return result

# [CMC FORGED HANDLER: {{entity}}]
from flask import request, jsonify
from core.cmc_core import cmc

CONTRACT = {
    "create": {
        "path": "/{{entity}}/create",
        "method": "POST",
        "input": {
{{contract_create_input}}
        },
        "output": {
            "status": "success",
            "id": "any"
        }
    },
    "get": {
        "path": "/{{entity}}/get/<entity_id>",
        "method": "GET",
        "output": {
{{contract_get_output}}
        }
    },
    "update": {
        "path": "/{{entity}}/update/<entity_id>",
        "method": "PUT",
        "input": {
{{contract_update_input}}
        },
        "output": {
            "status": "success"
        }
    },
    "delete": {
        "path": "/{{entity}}/delete/<entity_id>",
        "method": "DELETE",
        "output": {
            "status": "deleted"
        }
    }
}

# --- SQL QUERIES (CONST) ---
INSERT_SQL = "INSERT INTO {{table}} ({{col_names}}) VALUES ({{placeholders}}) RETURNING id;"
UPDATE_SQL = "UPDATE {{table}} SET {{update_set}} WHERE id = %s;"
DELETE_SQL = "DELETE FROM {{table}} WHERE id = %s;"

# [CMC MANUAL HOOK] Modify GET_SQL below to include Joins or optimized subqueries.
# Excluded FKs will appear in the hints section.
GET_SQL = """
SELECT {{all_col_names}} 
FROM {{table}} 
-- [JOIN PLACEHOLDER]
WHERE id = %s;
"""

{{fk_hints}}

@cmc.route(CONTRACT["create"], entity="{{entity}}", action="create")
def handle_{{entity}}_create():
    data = request.get_json()
    try:
        with cmc.db.cursor() as cur:
            values = [data.get(c) for c in {{columns_list}}]
            cur.execute(INSERT_SQL, values)
            new_id = cur.fetchone()[0]
            cmc.log.log(f"Entity {{entity}} forged: ID {new_id}")
            return jsonify(CONTRACT["create"]["output"] | {"id": new_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cmc.route(CONTRACT["get"], entity="{{entity}}", action="get")
def handle_{{entity}}_get(entity_id):
    try:
        with cmc.db.cursor() as cur:
            cur.execute(GET_SQL, (entity_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            return jsonify(dict(zip({{all_columns_list}}, row))), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cmc.route(CONTRACT["update"], entity="{{entity}}", action="update")
def handle_{{entity}}_update(entity_id):
    data = request.get_json()
    try:
        with cmc.db.cursor() as cur:
            values = [data.get(c) for c in {{columns_list}}]
            cur.execute(UPDATE_SQL, values + [entity_id])
            return jsonify(CONTRACT["update"]["output"]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cmc.route(CONTRACT["delete"], entity="{{entity}}", action="delete")
def handle_{{entity}}_delete(entity_id):
    try:
        with cmc.db.cursor() as cur:
            cur.execute(DELETE_SQL, (entity_id,))
            return jsonify(CONTRACT["delete"]["output"]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
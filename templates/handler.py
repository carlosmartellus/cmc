"""
[CMC CONTRACT EXAMPLE]
contract = {
    "user/register": {
        "method": "POST",
        "input": {
            "name": {
                "type": "str",
                "nullable": false
            },
            "password": "str"
        },
        "output": {
            "user_id": "int"
        }
    },
    "user/login/<id:int>": {
        "method": "GET",
        "input": {},
        "output": {
            "access": "bool"
        }
    }
}
"""

# [CMC FORGED HANDLER: {{entity}}]
from typing import Dict, Any
import json

TYPE_MAP = {
    "str": str, "int": int, "bool": bool, "float": float, "any": object
}

CONTRACT = {
    "{{entity}}/create": {
        "method": "POST",
        "input": {
{{contract_create_input}}
        },
        "output": {"status": "success", "id": "any"}
    },
    "{{entity}}/update/<id>": {
        "method": "PUT",
        "input": {
{{contract_update_input}}
        },
        "output": {"status": "success"}
    },
    "{{entity}}/get/<id>": {
        "method": "GET",
        "input": {},
        "output": {
{{contract_get_output}}
        }
    },
    "{{entity}}/delete/<id>": {
        "method": "DELETE",
        "input": {},
        "output": {"status": "deleted"}
    }
}

INSERT_SQL = "INSERT INTO {{table}} ({{col_names}}) VALUES ({{placeholders}}) RETURNING id;"
UPDATE_SQL = "UPDATE {{table}} SET {{update_set}} WHERE id = %s;"
GET_SQL    = "SELECT {{all_col_names}} FROM {{table}} WHERE id = %s;"
DELETE_SQL = "DELETE FROM {{table}} WHERE id = %s;"

def create_{{entity}}_routes(app):
    def handle_create(environ):
        try:
            length = int(environ.get('CONTENT_LENGTH', 0))
            body = environ['wsgi.input'].read(length) if length > 0 else b'{}'
            data = json.loads(body)
        except Exception:
            app.logger.warn("Create {{entity}} failed: Invalid JSON body")
            return '400 BAD REQUEST', {"error": "Invalid or missing JSON body"}

        schema = CONTRACT["{{entity}}/create"]["input"]
        for field, rules in schema.items():
            if field not in data and not rules.get("nullable", False):
                return '400 BAD REQUEST', {"error": f"Missing required field: {field}"}

        try:
            with app.db.cursor() as cur:
                values = [data.get(c) for c in {{columns_list}}]
                cur.execute(INSERT_SQL, values)
                new_id = cur.fetchone()[0]
                app.logger.log(f"Entity {{entity}} created (ID: {new_id})")
                return '201 CREATED', {"status": "success", "id": new_id}
        except Exception as e:
            app.logger.error(f"SQL Error (Create {{entity}}): {e}")
            return '500 INTERNAL SERVER ERROR', {"error": str(e)}

    def handle_get(environ, entity_id):
        try:
            with app.db.cursor() as cur:
                cur.execute(GET_SQL, (entity_id,))
                row = cur.fetchone()
                if not row:
                    return '404 NOT FOUND', {"error": f"{{entity}} {entity_id} not found"}
                
                app.logger.debug(f"Entity {{entity}} (ID: {entity_id}) retrieved")
                return '200 OK', dict(zip({{all_columns_list}}, row))
        except Exception as e:
            app.logger.error(f"SQL Error (Get {{entity}}): {e}")
            return '500 INTERNAL SERVER ERROR', {"error": str(e)}

    def handle_update(environ, entity_id):
        try:
            length = int(environ.get('CONTENT_LENGTH', 0))
            body = environ['wsgi.input'].read(length)
            data = json.loads(body)
        except Exception:
            return '400 BAD REQUEST', {"error": "Invalid JSON"}

        try:
            with app.db.cursor() as cur:
                values = [data.get(c) for c in {{columns_list}}]
                cur.execute(UPDATE_SQL, values + [entity_id])
                app.logger.log(f"Entity {{entity}} updated (ID: {entity_id})")
                return '200 OK', {"status": "success"}
        except Exception as e:
            app.logger.error(f"SQL Error (Update {{entity}}): {e}")
            return '500 INTERNAL SERVER ERROR', {"error": str(e)}

    def handle_delete(environ, entity_id):
        try:
            with app.db.cursor() as cur:
                cur.execute(DELETE_SQL, (entity_id,))
                app.logger.log(f"Entity {{entity}} deleted (ID: {entity_id})")
                return '200 OK', {"status": "deleted"}
        except Exception as e:
            app.logger.error(f"SQL Error (Delete {{entity}}): {e}")
            return '500 INTERNAL SERVER ERROR', {"error": str(e)}

    app.add_route('POST', '/{{entity}}/create', handle_create)
    app.add_route('GET', '/{{entity}}/get/<id>', handle_get)
    app.add_route('PUT', '/{{entity}}/update/<id>', handle_update)
    app.add_route('DELETE', '/{{entity}}/delete/<id>', handle_delete)
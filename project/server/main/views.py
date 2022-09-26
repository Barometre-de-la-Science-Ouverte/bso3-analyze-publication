import redis
import os
import string
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from project.server.main.tasks import create_task_analyze

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/analyze", methods=["POST"])
def run_task_analyze():
    args = request.get_json(force=True)
    hex_digits = string.hexdigits[0:16]
    for x in hex_digits:
        for y in hex_digits:
            prefix = f'{x}{y}'
            new_args = args.copy()
            new_args['prefix_uid'] = prefix
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue("analyze-publication", default_timeout=216000)
                task = q.enqueue(create_task_analyze, new_args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-hal")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)

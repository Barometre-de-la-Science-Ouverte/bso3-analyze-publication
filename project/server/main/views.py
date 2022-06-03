import redis
import os
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from project.server.main.tasks import create_task_analyze, create_task_tmp, create_task_download, create_task_split, import_es

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/tmp", methods=["POST"])
def run_task_tmp():
    args = request.get_json(force=True)
    if args.get('split', False):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("analyze-publication", default_timeout=216000)
            task = q.enqueue(create_task_split, 7000)
    if args.get('match', False):
        filenames = [f for f in os.listdir('/data/dump') if f[0]=='x' and len(f)==4]
        for f in filenames:
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue("analyze-publication", default_timeout=216000)
                task = q.enqueue(create_task_tmp, f'/data/dump/{f}')
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/analyze", methods=["POST"])
def run_task_analyze():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("analyze-publication", default_timeout=216000)
        task = q.enqueue(create_task_analyze, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/import", methods=["POST"])
def run_task_import():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("analyze-publication", default_timeout=216000)
        task = q.enqueue(import_es, args)
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

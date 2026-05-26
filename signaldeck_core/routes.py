import json
import signal
import sys
from typing import Any
from .models.manager import Manager
from flask import render_template, request, jsonify, abort, redirect, Flask

def merged_params() -> dict[str, Any]:
    p: dict[str, Any] = {}

    payload = request.form.get("payload")
    if payload:
        try:
            js = json.loads(payload)
            if isinstance(js, dict):
                p.update(js)
        except Exception:
            pass
    else:
        # Fallback für GET oder alte POSTs ohne payload
        p.update(request.values.to_dict(flat=True))

    return p


def register_routes(app: Flask) -> None:
    """
    Routes expect:
      app.extensions["signaldeck.manager"] = houseManager
    """

    @app.route("/", methods=["GET", "POST"])
    @app.route("/index", methods=["GET", "POST"])
    def index():
        houseManager: Manager = app.extensions["signaldeck.manager"]

        actions = request.form.getlist("actions")
        app.logger.info(actions)
        if actions:
            for act in actions:
                app.logger.info("Mod cron job")
                houseManager.setCronJob(act, request.form.get(f"cron_{act}"))
        groups = houseManager.getGroupsForPath("/")
        ui = houseManager.getJsAndCssFilesForGroups(groups)
        return render_template(
            "ui/index.html",
            paths=houseManager.getAvailablePaths(),
            groups=groups,
            title=houseManager.getTitleForPath("/"),
            additionalCssFiles=ui.css,
            additionalJsFiles=ui.js
        )

    @app.route("/<string:path>", methods=["GET"])
    def get_path(path: str):
        houseManager = app.extensions["signaldeck.manager"]
        groups = houseManager.getGroupsForPath(path)
        ui = houseManager.getJsAndCssFilesForGroups(groups)
        return render_template(
            "ui/index.html",
            paths=houseManager.getAvailablePaths(),
            groups=groups,
            title=houseManager.getTitleForPath(path),
            additionalCssFiles=ui.css,
            additionalJsFiles=ui.js
        )

    @app.route("/init")
    def init():
        houseManager = app.extensions["signaldeck.manager"]
        houseManager.reinit()
        return jsonify({"status": "ok"})

    @app.route("/run", methods=["GET", "POST"])
    def run():
        houseManager = app.extensions["signaldeck.manager"]

        data = merged_params()
        action = data.get("actionhash")
        app.logger.info(f"Receive action {action}")

        getparams = dict(data)
        getparams.pop("actionhash", None)

        res = houseManager.sendHash(
            action,
            params=getparams,
            file=request.files.get("file"),
        )

        app.logger.info(f"Action {action} is ready")
        if request.args.get("redirect", "no") == "home":
            return redirect("/")
        return res

    @app.route("/cronjob")
    def cronjob():
        houseManager = app.extensions["signaldeck.manager"]

        actionhash = request.args.get("elementhash")
        gr = houseManager.groupFromHash[actionhash]
        el = gr.elementByAction[houseManager.hashes[actionhash]]
        actions = gr.actionsByElement[el]

        return render_template(
            "core/cronjob.html",
            actions=actions,
            crons=houseManager.getCronsForActions(actions),
            action_url=f"{request.host}/run?actionhash=",
            additional_parameters="&redirect=home",
        )

    @app.route("/http/<string:name>", methods=["GET","POST"])
    def get_http(name: str):
        houseManager = app.extensions["signaldeck.manager"]

        data = houseManager.valueProvider.getHttp(name, **request.values.to_dict())
        if not data:
            abort(404, description=f"Element '{name}' not found")
        return jsonify(data)

    @app.route("/datastore/<store>/backup", methods=["GET"])
    def backup(store: str):
        houseManager = app.extensions["signaldeck.manager"]

        if store in houseManager.dataStore.keys():
            houseManager.dataStore[store].backup()
        return jsonify({"status": "started"})


def install_shutdown_handlers(app: Flask) -> None:
    """
    Graceful shutdown (SIGTERM/SIGINT).
    Expects: app.extensions["signaldeck.manager"] = houseManager
    """
    state = {"running": True}

    def _shutdown(signum, frame):
        if state["running"]:
            state["running"] = False
            app.logger.info("Shut down system..")
            try:
                houseManager = app.extensions.get("signaldeck.manager")
                if houseManager:
                    houseManager.shutdown()
            finally:
                sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

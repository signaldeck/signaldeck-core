from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from flask import render_template, url_for
from werkzeug.utils import secure_filename

from signaldeck_sdk import ApplicationContext, ValueProvider
from signaldeck_sdk.context import Renderer, FileService, UrlResolver
from .translator import TranslatorImpl


# -------------------------------------------------
# Renderer Implementation (Flask based)
# -------------------------------------------------

class FlaskRenderer(Renderer):
    """
    Concrete renderer used by the core runtime.
    Wraps flask.render_template.
    """

    def render(self, template: str, **kwargs) -> str:
        return render_template(template, **kwargs)


# -------------------------------------------------
# Renderer Implementation (Flask based)
# -------------------------------------------------

class UrlFlaskResolver(UrlResolver):
    """
    Concrete url resolver used by the core runtime.
    """

    def forFile(self, pluginName: str, filePath: str) -> str:
        return url_for(f'{pluginName}.static', filename=filePath)



# -------------------------------------------------
# File Service Implementation
# -------------------------------------------------

class LocalFileService(FileService):
    """
    Handles file persistence inside a configured base directory.
    """

    def __init__(self, logger: logging.Logger):

        self.logger = logger

    def save(self, file, path: str) -> str:
        """
        Save uploaded file to a safe location.
        Returns final absolute path.
        """

        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Optional: sanitize filename if user-controlled
        # filename = secure_filename(file.filename)
        # path = os.path.join(os.path.dirname(path), filename)

        file.save(path)

        self.logger.debug("File saved to %s", path)
        return path
    



# -------------------------------------------------
# Factory to build full ApplicationContext
# -------------------------------------------------

def build_application_context(
    *,
    values: ValueProvider,  # your ValueProvider instance
    logger: logging.Logger,
    lang: str = 'en',
    lang_fallback: str = 'en'
) -> ApplicationContext:
    """
    Creates a fully wired ApplicationContext instance
    used by processors.
    """

    renderer = FlaskRenderer()
    urlresolver = UrlFlaskResolver()
    file_service = LocalFileService(logger=logger)
    transl = TranslatorImpl(lang, lang_fallback)

    return ApplicationContext(
        renderer=renderer,
        url=urlresolver,
        files=file_service,
        translator=transl,
        values=values,
        logger=logger
    )

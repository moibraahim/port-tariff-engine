"""
Gemini API client wrapper.

Provides structured output extraction using Gemini's native capabilities.
Temperature=0 for reproducibility (idempotency guarantee from DDIA Ch 11).
"""

import os
import json
import logging
from pathlib import Path

import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class GeminiClient:
    """Wrapper around Google Gemini API for structured extraction."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not set. Provide via env or constructor.")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel("gemini-2.0-flash")

    def extract_structured(self, prompt: str, context: str) -> dict:
        """
        Send prompt + context to Gemini, expect JSON response.

        Uses temperature=0 for deterministic extraction.
        """
        full_prompt = f"{prompt}\n\n---\n\nDOCUMENT CONTENT:\n{context}"

        response = self.model.generate_content(
            full_prompt,
            generation_config=genai.GenerationConfig(
                temperature=0,
                max_output_tokens=8192,
                response_mime_type="application/json",
            ),
        )

        try:
            return json.loads(response.text)
        except json.JSONDecodeError:
            logger.error("Failed to parse Gemini response as JSON: %s", response.text[:500])
            raise

    def extract_with_image(self, prompt: str, image_path: str) -> dict:
        """Send prompt + image to Gemini for visual table extraction."""
        image_data = Path(image_path).read_bytes()
        image_part = {
            "mime_type": "image/png",
            "data": image_data,
        }

        response = self.model.generate_content(
            [prompt, image_part],
            generation_config=genai.GenerationConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )

        try:
            return json.loads(response.text)
        except json.JSONDecodeError:
            logger.error("Failed to parse Gemini vision response as JSON: %s", response.text[:500])
            raise

    def extract_text(self, prompt: str, context: str) -> str:
        """Send prompt + context, return plain text response."""
        full_prompt = f"{prompt}\n\n---\n\nDOCUMENT CONTENT:\n{context}"

        response = self.model.generate_content(
            full_prompt,
            generation_config=genai.GenerationConfig(temperature=0),
        )
        return response.text

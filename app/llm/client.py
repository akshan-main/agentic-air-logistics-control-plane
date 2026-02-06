# app/llm/client.py
"""
LLM Client - unified interface for OpenAI and Anthropic.

All agent reasoning goes through this client. No rule-based fallbacks.
"""

import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from ..settings import settings


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    raw_response: Any
    model: str
    usage: Dict[str, int]


class LLMClient:
    """
    Unified LLM client supporting OpenAI and Anthropic.

    Usage:
        client = LLMClient()
        response = client.complete(
            system="You are a risk assessment expert...",
            messages=[{"role": "user", "content": "Assess this..."}]
        )
    """

    def __init__(self, provider: Optional[str] = None):
        self.provider = provider or settings.llm_provider
        self._client = None
        self._init_client()

    def _init_client(self):
        """Initialize the appropriate client."""
        if self.provider == "anthropic":
            if not settings.anthropic_api_key:
                raise ValueError("ANTHROPIC_API_KEY not set")
            import anthropic
            self._client = anthropic.Anthropic(
                api_key=settings.anthropic_api_key,
                timeout=30.0,  # 30 second timeout (reduced from 60)
            )
            self.model = settings.anthropic_model
        else:  # openai
            if not settings.openai_api_key:
                raise ValueError("OPENAI_API_KEY not set")
            import openai
            self._client = openai.OpenAI(
                api_key=settings.openai_api_key,
                timeout=30.0,  # 30 second timeout (reduced from 60)
            )
            self.model = settings.openai_model

    def complete(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 4096,
        json_response: bool = False,
    ) -> LLMResponse:
        """
        Generate completion from LLM.

        Args:
            system: System prompt
            messages: List of message dicts with role/content
            temperature: Sampling temperature (0 = deterministic)
            max_tokens: Max response tokens
            json_response: If True, request JSON output

        Returns:
            LLMResponse with content and metadata
        """
        if self.provider == "anthropic":
            return self._complete_anthropic(system, messages, temperature, max_tokens)
        else:
            return self._complete_openai(system, messages, temperature, max_tokens, json_response)

    def _complete_anthropic(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ) -> LLMResponse:
        """Complete using Anthropic Claude."""
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=messages,
        )

        return LLMResponse(
            content=response.content[0].text,
            raw_response=response,
            model=self.model,
            usage={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }
        )

    def _complete_openai(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        json_response: bool,
    ) -> LLMResponse:
        """Complete using OpenAI GPT."""
        all_messages = [{"role": "system", "content": system}] + messages

        kwargs = {
            "model": self.model,
            "messages": all_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if json_response:
            kwargs["response_format"] = {"type": "json_object"}

        response = self._client.chat.completions.create(**kwargs)

        return LLMResponse(
            content=response.choices[0].message.content,
            raw_response=response,
            model=self.model,
            usage={
                "input_tokens": response.usage.prompt_tokens,
                "output_tokens": response.usage.completion_tokens,
            }
        )

    def complete_json(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Generate JSON response from LLM.

        Returns parsed JSON dict.
        """
        # Add JSON instruction to system prompt
        json_system = system + "\n\nRespond with valid JSON only. No markdown, no explanation."

        response = self.complete(
            system=json_system,
            messages=messages,
            temperature=temperature,
            json_response=(self.provider == "openai"),
        )

        # Parse JSON from response
        content = response.content.strip()

        # Handle markdown code blocks
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1])

        return json.loads(content)

    def complete_streaming(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ):
        """
        Generate streaming completion from LLM.

        Yields chunks of text as they arrive.
        """
        if self.provider == "anthropic":
            yield from self._stream_anthropic(system, messages, temperature, max_tokens)
        else:
            yield from self._stream_openai(system, messages, temperature, max_tokens)

    def _stream_openai(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ):
        """Stream using OpenAI."""
        all_messages = [{"role": "system", "content": system}] + messages

        stream = self._client.chat.completions.create(
            model=self.model,
            messages=all_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )

        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    def _stream_anthropic(
        self,
        system: str,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ):
        """Stream using Anthropic."""
        with self._client.messages.stream(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=messages,
        ) as stream:
            for text in stream.text_stream:
                yield text


# Global client instance
_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create global LLM client."""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client

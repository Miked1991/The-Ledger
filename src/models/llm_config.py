# src/models/llm_config.py
"""
LLM Configuration for AI Agents in The Ledger
Supports OpenRouter with Gemini Flash model
"""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv

load_dotenv()


class LLMConfig(BaseSettings):
    """Configuration for LLM integration"""
    
    # OpenRouter Configuration
    openrouter_api_key: str = Field(
        default=os.getenv("OPENROUTER_API_KEY", ""),
        description="OpenRouter API key"
    )
    openrouter_base_url: str = Field(
        default=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        description="OpenRouter base URL"
    )
    
    # Model Configuration
    model_name: str = Field(
        default=os.getenv("LLM_MODEL", "google/gemini-2.0-flash-exp:free"),
        description="LLM model to use"
    )
    temperature: float = Field(
        default=float(os.getenv("LLM_TEMPERATURE", "0.7")),
        ge=0.0,
        le=2.0,
        description="Temperature for response generation"
    )
    max_tokens: int = Field(
        default=int(os.getenv("LLM_MAX_TOKENS", "4096")),
        description="Maximum tokens in response"
    )
    top_p: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Top-p sampling"
    )
    
    # Agent-specific configurations
    agent_configs: Dict[str, Dict[str, Any]] = Field(
        default={
            "credit_analyst": {
                "system_prompt": "You are a credit analyst AI agent. Evaluate loan applications based on financial data and risk factors.",
                "temperature": 0.3,
                "max_tokens": 1024
            },
            "fraud_detector": {
                "system_prompt": "You are a fraud detection AI agent. Identify suspicious patterns and potential fraud indicators.",
                "temperature": 0.2,
                "max_tokens": 1024
            },
            "compliance_officer": {
                "system_prompt": "You are a compliance AI agent. Verify applications against regulatory requirements.",
                "temperature": 0.1,
                "max_tokens": 1024
            },
            "decision_orchestrator": {
                "system_prompt": "You are a decision orchestrator AI agent. Synthesize inputs from multiple agents and produce final recommendations.",
                "temperature": 0.4,
                "max_tokens": 2048
            }
        },
        description="Agent-specific LLM configurations"
    )
    
    # Performance
    request_timeout: int = Field(
        default=60,
        description="Request timeout in seconds"
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts"
    )
    
    class Config:
        env_prefix = "LLM_"
        case_sensitive = False


class LLMClient:
    """Client for interacting with LLM models via OpenRouter"""
    
    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._client = None
    
    def _get_client(self):
        """Get or create HTTP client"""
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(
                base_url=self.config.openrouter_base_url,
                timeout=self.config.request_timeout,
                headers={
                    "Authorization": f"Bearer {self.config.openrouter_api_key}",
                    "Content-Type": "application/json"
                }
            )
        return self._client
    
    async def generate(
        self,
        prompt: str,
        agent_type: Optional[str] = None,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs
    ) -> str:
        """
        Generate response from LLM
        
        Args:
            prompt: User prompt
            agent_type: Type of agent (uses config if provided)
            system_prompt: System prompt (overrides agent config)
            temperature: Temperature (overrides default)
            max_tokens: Max tokens (overrides default)
        
        Returns:
            Generated text response
        """
        client = self._get_client()
        
        # Get agent-specific configuration
        if agent_type and agent_type in self.config.agent_configs:
            agent_config = self.config.agent_configs[agent_type]
            system = system_prompt or agent_config.get("system_prompt", "")
            temp = temperature or agent_config.get("temperature", self.config.temperature)
            max_tok = max_tokens or agent_config.get("max_tokens", self.config.max_tokens)
        else:
            system = system_prompt or "You are a helpful AI assistant."
            temp = temperature or self.config.temperature
            max_tok = max_tokens or self.config.max_tokens
        
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        
        try:
            response = await client.post(
                "/chat/completions",
                json={
                    "model": self.config.model_name,
                    "messages": messages,
                    "temperature": temp,
                    "max_tokens": max_tok,
                    "top_p": self.config.top_p,
                    **kwargs
                }
            )
            response.raise_for_status()
            data = response.json()
            
            return data["choices"][0]["message"]["content"]
            
        except Exception as e:
            print(f"LLM generation failed: {e}")
            raise
    
    async def generate_structured(
        self,
        prompt: str,
        schema: Dict[str, Any],
        agent_type: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Generate structured JSON response from LLM
        
        Args:
            prompt: User prompt
            schema: JSON schema for output
            agent_type: Type of agent
        
        Returns:
            Structured response as dict
        """
        # Add schema to prompt
        structured_prompt = f"{prompt}\n\nRespond with valid JSON matching this schema: {schema}"
        
        response = await self.generate(
            structured_prompt,
            agent_type=agent_type,
            **kwargs
        )
        
        # Parse JSON response
        import json
        try:
            # Extract JSON from response (may contain markdown)
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                response = response.split("```")[1].split("```")[0]
            
            return json.loads(response.strip())
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            print(f"Raw response: {response}")
            raise
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()


# Singleton instance
_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create LLM client instance"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


async def close_llm_client():
    """Close LLM client"""
    global _llm_client
    if _llm_client:
        await _llm_client.close()
        _llm_client = None
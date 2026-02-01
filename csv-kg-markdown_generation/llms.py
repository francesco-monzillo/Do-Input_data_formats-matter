import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_deepseek import ChatDeepSeek
from langchain_anthropic import ChatAnthropic

load_dotenv()
openai_api_key = os.getenv('OPENAI_API_KEY')
gemini_key = os.getenv('GOOGLE_API_KEY')
deepseek_key = os.getenv('DEEPSEEK_API_KEY')
anthropic_key = os.getenv('ANTHROPIC_API_KEY')


class LLMS:
    def __init__(self, model, prompt_template, quality_report=None, quality_documentation=None, question=None):
        self.chat = return_chat_model(model)
        self.prompt_template = prompt_template
        self.quality_report = quality_report
        self.quality_documentation = quality_documentation
        self.question = question

    def execute_question(self):
        chain = self.prompt_template | self.chat

        result =  chain.invoke({"quality_report": self.quality_report, "quality_documentation": self.quality_documentation, "question": self.question})
        return result.content

    def execute_verbalization(self):
        chain = self.prompt_template | self.chat

        result =  chain.invoke({"quality_report": self.quality_report, "quality_report_verbalized_example": self.question, "quality_documentation": self.quality_documentation})
        return result.content

def return_chat_model(model_name, temperature=0.0):
    if 'gpt' in model_name:
        if temperature == 0.0:
            return ChatOpenAI(model=model_name, openai_api_key=openai_api_key, temperature=temperature, reasoning_effort="medium", verbosity="low")
        else:
            return ChatOpenAI(model=model_name, openai_api_key=openai_api_key)
    elif 'gemini' in model_name:
        return ChatGoogleGenerativeAI(model=model_name, google_api_key=gemini_key, max_tokens=None, temperature=temperature)
    elif 'deepseek' in model_name:
        return ChatDeepSeek(model=model_name, temperature=temperature, max_tokens=None, api_key=deepseek_key)
    elif 'claude' in model_name:
        return ChatAnthropic(model=model_name, anthropic_api_key=anthropic_key, temperature=temperature)
    else:
        raise ValueError(f"Model {model_name} is not supported.")
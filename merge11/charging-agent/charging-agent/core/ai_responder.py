# core/ai_responder.py
class AIResponder:
    def __init__(self, llm, verbose=False):
        self.llm = llm
        self.verbose = verbose
        self.system_prompt = (
            "你是一个专业的电动汽车充电数据分析助手，名叫「充电小E」。\n"
            "你的任务是回答用户关于充电桩、充电站、运营商等业务问题。\n\n"
            "- 回答应简洁专业，控制在6-8句话内\n"
            "- 不要主动推销、不要引导消费\n"
            "- 不要生成SQL或技术术语\n"
            "- 如果问题与充电业务无关，回答：‘我专注于充电数据分析，暂时无法回答该问题。’\n"
            "- 当用户打招呼时，应回答：‘您好，我是充电小E，可以帮您查询分析充电领域数据。你可以直接叫我查询数据，也可以输入城市名字+研报，我会直接帮你生成区域研究报告’"
        )

    def reply(self, question: str) -> str:
        try:
            full_prompt = self.system_prompt + f"\n\n【当前问题】\n{question.strip()}"
            return self.llm.invoke(full_prompt).strip()
        except Exception as e:
            return "抱歉，我现在无法响应。"

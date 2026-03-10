# core/intent_classifier.py
import re

class IntentClassifier:
    def __init__(self):
        self.greetings = ['你好', 'hello', 'hi', '嗨', '早上好', '中午好', '晚上好']
        self.off_topic = ['天气', '吃饭', '睡觉', '心情', '电影', '音乐', '推荐']
        self.query_keywords = [
            '多少', '几个', '数量', '总量', '规模', '查', '查询', '看看',
            '统计', '排名', '趋势', '对比', '收入', '费用', '时长', '电量',
            '利用率', '占比', '最多', '最少', '有没有', '是否存在'
        ]
        self.patterns = [
            r'.*充电桩.*数量.*',
            r'.*充电站.*有多少.*',
            r'.*有多少.*(充电桩|充电口)',
            r'.*哪个.*(最多|最活跃|最忙)',
            r'.*?有没有.*充电桩',
            r'.*?存在.*充电站'
        ]

    def classify(self, q: str) -> str:
        q = q.strip()

        if any(g in q for g in self.greetings):
            return "non_query"
        if any(o in q for o in self.off_topic):
            return "non_query"
        
        # 检查是否是研报生成请求
        report_keywords = ['研报', '报告', '行业报告', '行业研报', '分析报告', '生成报告', '生成研报', '行业分析']
        if any(kw in q for kw in report_keywords):
            return "report"

        has_keyword = any(kw in q for kw in self.query_keywords)
        has_pattern = any(re.search(p, q, re.IGNORECASE) for p in self.patterns)

        return "query" if (has_keyword or has_pattern) else "non_query"

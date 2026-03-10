# core/response_formatter.py
import re
from core.knowledge_base import KnowledgeBase

class ResponseFormatter:
    @staticmethod
    def format(question: str, result) -> str:
        """
        将数据库查询结果转为自然语言回答
        :param question: 用户原始问题
        :param result: SQL 查询结果（可能为字符串、元组、列表等）
        :return: 自然语言格式的回答
        """
        # === 第一步：清理并解析原始结果 ===
        raw_str = str(result).strip()

        # 处理空值或无效数据
        if not raw_str or "None" in raw_str or len(raw_str) == 0 or raw_str in ["[]", "[()]", "()"]:
            return "暂无相关数据。"

        # 提取数字：支持 [(1886,)], (237,), [123] 等格式
        count = None
        try:
            numbers = re.findall(r'\d+', raw_str)
            if numbers:
                count = int(numbers[0])  # 取第一个匹配到的数字
        except Exception as e:
            pass

        # 如果正则失败，尝试 eval 解析结构化数据
        if count is None:
            try:
                parsed = eval(raw_str)
                if isinstance(parsed, list) and len(parsed) > 0:
                    first_row = parsed[0]
                    if isinstance(first_row, tuple) and len(first_row) > 0:
                        count = int(first_row[0])
            except:
                pass

        # 格式化带千分位的数字
        formatted_count = f"{count:,}" if count is not None else None

        # === 第二步：根据问题类型生成智能回答 ===

        # --- 场景1：数量类问题 ---
        if any(kw in question for kw in ['多少', '几个', '数量', '保有量', '总量', '规模']):
            location = ""
            operator = ""
            obj = ""

            # 匹配常见区域关键词（使用知识库）
            for key, full_name in KnowledgeBase.LOCATION_NICKNAMES.items():
                if key in question:
                    # 格式化输出（提取区域名称，去掉市和省）
                    if "区" in full_name:
                        location = f"在{full_name.split('市')[-1]}" if "市" in full_name else f"在{full_name}"
                    else:
                        location = f"在{full_name}"
                    break

            # 匹配运营商简称（使用知识库）
            for abbr, full in KnowledgeBase.OPERATOR_NICKNAMES.items():
                if abbr in question:
                    operator = full
                    break

            # 判断对象类型
            if '充电站' in question:
                obj = '充电站'
            elif '运营商' in question:
                obj = '运营商'
            else:
                obj = '充电桩'

            # 构造最终回答
            if operator and location:
                return f"{operator}{location}共有 {formatted_count} 台{obj}。"
            elif operator:
                return f"{operator}共有 {formatted_count} 台{obj}。"
            elif location:
                return f"{location}共有 {formatted_count} 台{obj}。"
            else:
                return f"符合条件的{obj}共有 {formatted_count} 台。"

        # --- 场景2：排名/最忙/最多 ---
        if any(kw in question for kw in ['排名', '最多', '最活跃', '最忙', '领先', '榜首']):
            parts = [p.strip().strip("'\"") for p in raw_str.split(',') if p.strip()]
            if len(parts) >= 2:
                name = parts[0]
                value = parts[1]
                try:
                    num_val = float(value)
                    unit = "次" if num_val == int(num_val) else "度"
                    return f"表现最突出的是「{name}」，数值为 {num_val:,.0f}{unit}。"
                except:
                    return f"排名第一的是「{name}」。"
            return f"查询结果显示：{raw_str}"

        # --- 场景3：收入/费用/金额 ---
        if any(kw in question for kw in ['收入', '费用', '金额', '成本', '总花费', '花了']):
            try:
                amount = float(raw_str)
                return f"总金额为 ¥{amount:,.2f} 元。"
            except:
                pass

        # --- 场景4：时长/时间 ---
        if any(kw in question for kw in ['时长', '持续时间', '用了多久', '耗时']):
            try:
                hours = float(raw_str)
                return f"平均充电时长约为 {hours:.1f} 小时。"
            except:
                pass

        # === 默认兜底 ===
        return f"查询结果：{raw_str}"

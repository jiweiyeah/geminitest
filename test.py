 #!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
裁判文书批量处理工具
批量处理Excel中的裁判文书并输出结构化数据
"""

import google.generativeai as genai
import sys
import pandas as pd
import json
import os
import time
import concurrent.futures
from tqdm import tqdm
import openpyxl

# --- API密钥配置 ---
genai.configure(api_key='AIzaSyDFNnDMZXXAFkSgtw86RcafPrquJjNYAks')  # 请替换为您的API密钥

# --- 文件路径定义 ---
file_path = '.'  # 默认为当前目录，根据实际情况修改
_UPSTREAM_FILE_PATH = file_path + '/upstream_crime_types.md'
INPUT_EXCEL_PATH = file_path + '/textExcel.xlsx'
OUTPUT_EXCEL_PATH = file_path + '/output.xlsx'

# Excel列配置
DOCUMENT_COLUMN_INDEX = 1  # B列对应的索引是1 (A列是0)
DOCUMENT_COLUMN_NAME = '文书内容'

# 并发和重试设置
CONCURRENT_LIMIT = 5
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2

def _load_content_from_file(file_path: str) -> str:
    """
    从指定文件路径读取文件内容并返回。
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"--- 成功读取静态Prompt文件: {file_path} ---")
        return content
    except FileNotFoundError:
        print(f"错误：文件 '{file_path}' 未找到。请确保文件已上传并路径正确。")
        sys.exit(1)
    except Exception as e:
        print(f"读取文件 '{file_path}' 时发生错误: {e}")
        sys.exit(1)

# 用户Prompt模板定义
user_prompt_template = """
# 裁判文书分析专家指令

## 角色定位
你是一名专业的、严谨的裁判文书分析专家。

## 主要任务
- 深度分析用户提供的裁判文书内容
- {UPSTREAM_CRIME_TYPES_PROMPT} 提供法律解释和上游犯罪类型表，{document_content} 提供待分析的裁判文书内容
- 根据中国刑法相关条款判断案件是否涉及洗钱犯罪
  - 第191条（洗钱罪）
  - 第312条（掩饰、隐瞒犯罪所得、犯罪所得收益罪）
  - 第349条（窝藏、转移、隐瞒毒品、毒赃罪）
- 提取并结构化呈现案件的所有关键信息

## 输出规范要求
1. **所有字段必须输出**：无对应信息时填写"不涉及"、0或null
2. **金额统一转换**：所有金额折合为人民币，以当前最新汇率进行判断，单位为万元，保留两位小数
3. **格式严格规范**：使用英文双引号包裹所有键名和字符串值，禁用中文引号
4. **纯JSON格式**：输出必须是纯JSON格式，无额外解释说明

## 处理步骤
1. 识别所有涉案金额
2. 计算违法所得总额和清洗金额
3. 提取案件关键信息
4. 输出JSON结果

## 信息来源
- {UPSTREAM_CRIME_TYPES_PROMPT} 提供法律解释和上游犯罪类型表
- {document_content} 提供待分析的裁判文书内容

## 输出JSON结构

```json
{{
"1. 罪名": "涉及洗钱犯罪的请按下列分类填写相应字母，不涉及则填写\\\"不涉及\\\"：\\nA. 刑法第191条洗钱罪\\nB. 刑法第312条掩饰、隐瞒犯罪所得、犯罪所得收益罪\\nC. 刑法第349条窝藏、转移、隐瞒毒品、毒赃罪",
"2. 判决书文号": "填写判决的文书编号",
"3. 洗钱案件名称": "请按\\\"某办案单位：某地+主体/代号+犯罪类型+案\\\"格式填写，如果不是洗钱案件，也请填写具体的犯罪类型名称。\\n示例: \\\"广州市公安局：广东广州汪某XX案\\\", \\\"广东深圳李某洗钱案\\\"",
"4. 涉及上游犯罪类型": "请按下列分类填写相应字母，可多选（可查找\\\"判决如下\\\"开始的文字）。如果是非 A-M 类的罪名，即为 N 类其他犯罪罪名，请注明具体的犯罪罪名，例如\\\"N（XX罪）\\\"\\nA. 走私、贩卖、运输、制造毒品罪\\nB. 走私罪\\nC. 黑社会性质犯罪\\nD. 恐怖活动犯罪\\nE. 贪污贿赂犯罪(含商业贿赂)\\nF. 涉非法集资罪\\nG. 破坏金融管理秩序罪（不含洗钱罪）\\nH. 金融诈骗犯罪(不含集资诈骗)\\nI. 组织、领导传销活动罪\\nJ. 赌博罪\\nK. 危害税收征管罪\\nL. 非法经营罪（仅指地下钱庄）\\nN. 其他犯罪类型(请注明具体罪名)",
"5. 一审判决时间": "请按\\\"****年**月****日\\\"格式填写。\\n示例：\\\"2021年1月1日\\\"",
"6. 判决法院所在地区": "请填写到市级。\\n示例：\\\"广东省广州市\\\"",
"7. 认定违法所得金额": "外币折合成人民币，单位（万元），整个案件的违法犯罪所得。常见\\\"审理\\\"，\\\"查明\\\",\\\"公诉机关指控\\\",\\\"经审理查明\\\"等字段",
"8. 其中，隐匿或清洗犯罪所得金额": "外币折合成人民，币以当前最新汇率进行判断，单位（万元）。经审理查明\"段落中的资金描述,\"事实认定\"部分的资金流向说明\"法院认为\"中关于犯罪行为的认定。案件涉及同一笔犯罪所得资金在不同账户多次转移的，统计时请剔除重复项，勿对该笔资金进行多次统计。",
"9. 涉及行业或领域": "请按下列分类填写相应字母（数字），可多选：\\nA. 金融业（A1.银行业；A2.证券业；A3.期货业；A4.基金业；A5.保险业；A6.信托业）\\nB. 特定非金融行业（B1.支付服务业；B2.房地产业；B3.贵金属、珠宝业；B4.法律服务业；B5.会计服务业）\\nC. 其他高风险行业或领域（C1.影视娱乐业；C2.进出口贸易；C3.批发零售；C4.博彩业；C5.高科技业;C6.拍卖业；C7.典当业；C8.企业登记代理服务业；C9.公证业）\\nD. 虚拟资产领域\\nE. 数字货币领域\\nF. 一般公司企业(不涉及 A、B、C、D、E，则为 F)",
"10. 涉及洗钱手段": "选出具体的相关手段，没有涉及洗钱相关手段，填写\\\"不涉及\\\"。请按下列分类填写相应字母（及数字），可多选：\\nA. 提供资金帐户（A1.使用他人银行账户；A2.使用他人微信、支付宝等支付账户；A3.使用他人证券账户）\\nB. 将财产转换为现金、金融票据、有价证券（B1.提取现金；B2.使用汇票、本票、支票等金融票据；B3.使用存单、国库券等有价证券）\\nC. 通过转帐或者其他支付结算方式转移资金（C1.频繁或大额向其他银行账户转帐；C2.频繁或大额向其他支付账户转帐）\\nD. 跨境转移资产（D1.外汇兑换或跨境汇入、汇出；D2.通过地下钱庄等渠道）\\nE. 以其他方法掩饰、隐瞒犯罪所得及其收益的来源和性质（E1.通过典当、租赁、买卖、投资、拍卖等方式，转移、转换犯罪所得及其收益的；E2.通过与商场、饭店、娱乐场所等现金密集型场所的经营收入相混合的方式，转移、转换犯罪所得及其收益的；E3.通过虚构交易、虚设债券债务、虚假担保、虚假收入等方式，将犯罪所得及其收益转换为\\\"合法\\\"财物的；E4.通过买卖彩票、奖券等方式，转换犯罪所得及其收益的；E5.通过赌博方式，将犯罪所得及其收益转换为赌博收益的；E6.通过\\\"虚拟货币\\\"交易方式，转移、转换犯罪所得及其收益的；E7.通过前述规定以外的方式转移、转换犯罪所得及其收益的）",
"11. 隐匿或清洗犯罪所得方式": "外币折合为人民币请按下列分类填写相应字母，并且注明金额。如果是 K, L, M，注明种类和金额。可多选，如未隐匿或者清洗，填写\\\"不涉及\\\"：\\nA. 本币现金或票据（注明金额，单位人民币万元）\\nB. 外币现金或票据（注明金额，单位人民币万元）\\nC. 银行存款（注明金额，单位人民币万元）\\nD. 银行理财（注明金额，单位人民币万元）\\nE. 证券（含股票、期货、债券)（注明金额，单位人民币万元）\\nF. 基金（注明金额，单位人民币万元）\\nG. 保险（注明金额，单位人民币万元）\\nH. 信托（注明金额，单位人民币万元）\\nI. 支付账户（注明金额，单位人民币万元）\\nJ. 其他金融资产（注明种类及金额、份额，金额单位：人民币万元）\\nK. 房产（注明数量和面积）\\nL. 珠宝贵金属（注明种类及金额，金额单位：人民币万元）\\nM. 其他实物资产（注明种类及金额，金额单位：人民币万元）",
"12. 判决案例涉及主体数量": "请统计判决书中提及的被告人（包括个人和公司）总数，并填写数字。在\\\"判决如下\\\"部分，会列出具体的被告人及其被判决的罪名。",
"13. 跨境资金涉及境外地区情况": "请按下列分类填写相应字母，没有发生则填写\\\"不涉及\\\"：\\nA. 香港\\nB. 澳门\\nC. 均涉及",
"14. 资金跨境流动方向": "请按下列分类填写相应字母，没有跨境流动填写\\\"不涉及\\\"：\\nA. 资金由境内流向境外\\nB. 资金由境外流向境内\\nC. 存在资金跨境双向流动",
"15. 跨境资金总金额": "折合成人民币，单位（万元），未涉及则填写 0",
"16. 其中涉及香港地区ATM取现金额": "折合成人民币，单位（万元），未涉及则填写 0",
"17. 其中涉及澳门地区ATM取现金额": "折合成人民币，单位（万元），未涉及则填写 0"
}}
"""

def process_row_sync(index, row_data, model, user_prompt_template, UPSTREAM_CRIME_TYPES_PROMPT):
    """
    处理单行数据的函数，包含自动重试逻辑
    """
    document_text = row_data['文书内容']
    if pd.isna(document_text) or not str(document_text).strip():
        return {"error": "文书内容为空"}

    final_user_prompt = user_prompt_template.format(
        UPSTREAM_CRIME_TYPES_PROMPT=UPSTREAM_CRIME_TYPES_PROMPT,
        document_content=str(document_text)
    )

    for attempt in range(MAX_RETRIES):
        try:
            response = model.generate_content(
                contents=[{'role': 'user', 'parts': [final_user_prompt]}],
                generation_config=genai.types.GenerationConfig(temperature=0.0),
                request_options={'timeout': 120}
            )

            cleaned_response = response.text.strip().lstrip('```json').rstrip('```')
            parsed_json = json.loads(cleaned_response)
            return parsed_json

        except (json.JSONDecodeError) as e:
            return {"error": f"JSON解析失败: {e}", "raw_response": getattr(response, 'text', 'No response text available')}

        except Exception as e:
            error_message = f"处理时发生未知错误: {type(e).__name__} - {e}"
            print(f"  - 第 {index + 1} 行处理失败 (尝试 {attempt + 1}/{MAX_RETRIES})。错误: {type(e).__name__}。将在 {RETRY_BACKOFF_FACTOR * (attempt + 1)} 秒后重试...")

            if attempt == MAX_RETRIES - 1:
                print(f"  - 第 {index + 1} 行已达最大重试次数，处理失败。")
                return {"error": f"多次重试后失败: {error_message}"}

            time.sleep(RETRY_BACKOFF_FACTOR * (attempt + 1))

    return {"error": "未知原因导致函数退出"}

def main():
    """
    主函数
    """
    # 加载静态Prompt内容
    UPSTREAM_CRIME_TYPES_PROMPT = _load_content_from_file(_UPSTREAM_FILE_PATH)
    
    # 初始化模型
    system_prompt = "你是一个专业的法律文件分析助手。你的任务是根据用户需求提取文档信息，并以纯JSON，其中字段是中文的形式列出。不要添加任何无关的评论或解释，确保输出仅包含JSON内容。"
    model = genai.GenerativeModel(
        model_name='gemini-2.5-flash-preview-05-20',
        system_instruction=system_prompt
    )
    
    # 读取Excel文件
    try:
        df = pd.read_excel(INPUT_EXCEL_PATH, usecols=[DOCUMENT_COLUMN_INDEX])
        df.columns = ['文书内容']
        print(f"\n--- 成功读取Excel文件: {INPUT_EXCEL_PATH} ---")
        print(f"文件中共有 {len(df)} 个案例需要处理。")
    except FileNotFoundError:
        print(f"错误：Excel文件 '{INPUT_EXCEL_PATH}' 未找到。")
        return
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        return

    # 使用ThreadPoolExecutor并发处理
    print(f"\n--- 开始并发处理案例 (线程池限制: {CONCURRENT_LIMIT}, 最大重试: {MAX_RETRIES}) ---")

    results_map = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_LIMIT) as executor:
        future_to_index = {executor.submit(process_row_sync, index, row, model, user_prompt_template, UPSTREAM_CRIME_TYPES_PROMPT): index for index, row in df.iterrows()}

        for future in tqdm(concurrent.futures.as_completed(future_to_index), total=len(df), desc="处理进度"):
            index = future_to_index[future]
            try:
                result = future.result()
                results_map[index] = result
            except Exception as exc:
                print(f'  - 第 {index + 1} 行在执行时产生严重异常: {exc}')
                results_map[index] = {"error": f"执行时异常: {exc}"}

    # 整合结果并保存到新Excel文件
    print("\n--- 所有案例处理完成，正在整合结果... ---")

    all_results = [results_map[i] for i in sorted(results_map.keys())]
    results_df = pd.json_normalize(all_results)
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    try:
        final_df.to_excel(OUTPUT_EXCEL_PATH, index=False, engine='openpyxl')
        print(f"\n--- 大功告成！结果已保存至: {OUTPUT_EXCEL_PATH} ---\n")
    except Exception as e:
        print(f"\n保存结果到Excel时发生错误: {e}")

if __name__ == "__main__":
    main()
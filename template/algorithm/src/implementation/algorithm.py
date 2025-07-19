import os
import json
import csv
import re
from typing import Dict, Any, List, Union

# ==============================================================================
# 用户自定义区域：请根据你的需求修改以下配置和函数
# ==============================================================================

# 配置输出文件夹
OUTPUT_DIR = "filtered_and_chunked_data"

# 隐私信息过滤规则
# 假设的敏感字段名，这些字段的值将被完全移除或替换为占位符
# (这里我们主要针对人名进行替换)
SENSITIVE_FIELD_NAMES = ["Name"] # 例如：Appointee.Name

# 用于替换隐私信息的占位符
ANONYMIZATION_PLACEHOLDER = "[ANONYMIZED_PERSON]"

# ==============================================================================
# 核心算法逻辑：通常不需要修改
# ==============================================================================

def anonymize_text(text: str, sensitive_names: List[str]) -> str:
    """
    对文本进行匿名化处理，替换敏感人名。
    Args:
        text (str): 原始文本。
        sensitive_names (List[str]): 需要匿名化的人名列表。
    Returns:
        str: 匿名化后的文本。
    """
    anonymized_text = text
    for name in sensitive_names:
        # 使用正则表达式进行不区分大小写的替换，并处理可能的标点符号或词边界
        # 确保只替换完整的人名，避免替换部分词语
        pattern = r'\b' + re.escape(name) + r'\b'
        anonymized_text = re.sub(pattern, ANONYMIZATION_PLACEHOLDER, anonymized_text, flags=re.IGNORECASE)
    return anonymized_text

def filter_and_chunk_json_data(data: Dict[str, Any], original_file_name: str) -> List[Dict[str, Any]]:
    """
    处理 JSON 格式的输入数据，进行隐私过滤和分块，并按指定格式构建。
    Args:
        data (Dict[str, Any]): 原始 JSON 数据。
        original_file_name (str): 原始输入文件名，用于生成 Source 元数据。
    Returns:
        List[Dict[str, Any]]: 经过隐私过滤和分块后的数据列表。
                               每个字典代表一个符合输出格式的结构。
    """
    filtered_output_units = [] # 存储每个 Decree/Order 对应的输出单元

    # 收集所有潜在的敏感人名，以便在文章内容中进行匿名化
    all_sensitive_names = []
    for item_list_key in ['Decrees', 'Orders']:
        if item_list_key in data:
            for item in data[item_list_key]:
                if 'Appointee' in item and 'Name' in item['Appointee']:
                    all_sensitive_names.append(item['Appointee']['Name'])
                if 'Replacement' in item.get('Appointee', {}) and item['Appointee']['Replacement']:
                     all_sensitive_names.append(item['Appointee']['Replacement']) # 如果有Replacement字段

    # 处理 Decrees
    if 'Decrees' in data:
        for i, decree in enumerate(data['Decrees']):
            unit_metadata = {
                "Source": original_file_name,
                "Type": "Decree",
                "Date": decree.get('Date', 'N/A'),
                "Title (FR)": decree.get('Title_FR', 'N/A'),
                "Title (EN)": decree.get('Title_EN', 'N/A')
            }
            
            unit_chunks = []
            if 'Articles' in decree:
                for j, article in enumerate(decree['Articles']):
                    # 匿名化文章内容
                    anonymized_content_fr = anonymize_text(article.get('Content_FR', ''), all_sensitive_names)
                    anonymized_content_en = anonymize_text(article.get('Content_EN', ''), all_sensitive_names)
                    
                    unit_chunks.append({
                        "Chunk_ID": f"{j+1:03d}", # 格式化为 001, 002
                        "Content (FR)": anonymized_content_fr,
                        "Content (EN)": anonymized_content_en
                    })

            # 对Appointee Name进行过滤，仅在元数据部分使用占位符
            # 这里的思路是，Appointee Name是元数据，直接替换，内容中的人名通过anonymize_text处理
            processed_appointee_name = decree.get('Appointee', {}).get('Name', 'N/A')
            if processed_appointee_name != 'N/A':
                 unit_metadata["Appointee Name"] = ANONYMIZATION_PLACEHOLDER
            else:
                 unit_metadata["Appointee Name"] = processed_appointee_name # 保持N/A
            
            # 示例：如果 Appointee 还有其他字段，可以考虑在此处添加
            # if 'Title' in decree.get('Appointee', {}):
            #     unit_metadata["Appointee Title"] = decree['Appointee']['Title']

            filtered_output_units.append({
                "metadata": unit_metadata,
                "chunks": unit_chunks
            })

    # 处理 Orders
    if 'Orders' in data:
        for i, order in enumerate(data['Orders']):
            unit_metadata = {
                "Source": original_file_name,
                "Type": "Order",
                "Date": order.get('Date', 'N/A'),
                "Title (FR)": order.get('Title_FR', 'N/A'),
                "Title (EN)": order.get('Title_EN', 'N/A')
            }
            
            unit_chunks = []
            if 'Articles' in order:
                for j, article in enumerate(order['Articles']):
                    # 匿名化文章内容
                    anonymized_content_fr = anonymize_text(article.get('Content_FR', ''), all_sensitive_names)
                    anonymized_content_en = anonymize_text(article.get('Content_EN', ''), all_sensitive_names)

                    unit_chunks.append({
                        "Chunk_ID": f"{j+1:03d}", # 格式化为 001, 002
                        "Content (FR)": anonymized_content_fr,
                        "Content (EN)": anonymized_content_en
                    })

            processed_appointee_name = order.get('Appointee', {}).get('Name', 'N/A')
            if processed_appointee_name != 'N/A':
                 unit_metadata["Appointee Name"] = ANONYMIZATION_PLACEHOLDER
            else:
                 unit_metadata["Appointee Name"] = processed_appointee_name # 保持N/A
            
            # 示例：如果 Appointee 还有其他字段，可以考虑在此处添加
            # if 'Title' in order.get('Appointee', {}):
            #     unit_metadata["Appointee Title"] = order['Appointee']['Title']
            
            filtered_output_units.append({
                "metadata": unit_metadata,
                "chunks": unit_chunks
            })

    return filtered_output_units

def filter_and_chunk_csv_data(data: List[Dict[str, Any]], original_file_name: str) -> List[Dict[str, Any]]:
    """
    处理 CSV 格式的输入数据，进行隐私过滤和分块。
    （CSV 数据的具体过滤和分块逻辑需要根据你的 CSV 样本格式来确定）
    Args:
        data (List[Dict[str, Any]]): 原始 CSV 数据，每行是一个字典。
        original_file_name (str): 原始输入文件名。
    Returns:
        List[Dict[str, Any]]: 经过隐私过滤和分块后的数据列表。
    """
    filtered_output_units = []
    # 假设CSV的每一行可以作为一个独立的文档单元
    # 你需要定义CSV中哪些列是敏感的，以及如何从中提取元数据和内容
    
    # 示例：如果CSV的列包含 'Name', 'Date', 'Description'
    # 并且你想把 Description 作为Content (EN)，其他作为元数据
    
    # 收集CSV中可能存在的敏感人名列
    sensitive_names_csv = []
    for row in data:
        if 'Name' in row and row['Name']: # 假设CSV中有'Name'列作为隐私信息
            sensitive_names_csv.append(row['Name'])
        # 添加其他可能包含人名的列

    for i, row in enumerate(data):
        unit_metadata = {
            "Source": original_file_name,
            "Type": "CSV_Row", # 或者根据CSV内容定义更具体的类型
            "Row_ID": str(i + 1) # 行号作为ID
        }
        
        # 假设CSV中有一个 'Date' 列
        if 'Date' in row:
            unit_metadata['Date'] = row['Date']

        # 假设CSV中有一个 'Subject' 列作为标题
        if 'Subject' in row:
            unit_metadata['Title'] = row['Subject']

        # 过滤敏感字段
        processed_row_content = row.copy()
        if 'Name' in processed_row_content:
            processed_row_content['Name'] = ANONYMIZATION_PLACEHOLDER

        # 匿名化文本内容（假设有一个 'Text_Content' 列）
        if 'Text_Content' in processed_row_content:
            processed_row_content['Text_Content'] = anonymize_text(processed_row_content['Text_Content'], sensitive_names_csv)

        # 构建分块内容，这里把整行（过滤后）作为一块，或者把特定文本列作为块
        unit_chunks = []
        if 'Text_Content' in processed_row_content:
            unit_chunks.append({
                "Chunk_ID": "001",
                "Content (EN)": processed_row_content['Text_Content'] # 假设这一列是英文内容
                # 如果有法文内容列，也在这里添加
            })
        else: # 如果没有特定的文本内容列，可以将整行转换成字符串作为内容
            chunk_content = json.dumps(processed_row_content, ensure_ascii=False)
            unit_chunks.append({
                "Chunk_ID": "001",
                "Content (EN)": f"CSV Row data: {chunk_content}"
            })

        filtered_output_units.append({
            "metadata": unit_metadata,
            "chunks": unit_chunks
        })
    return filtered_output_units


def write_chunks_to_files(chunks_data: List[Dict[str, Any]], output_sub_dir: str):
    """
    将处理后的数据块写入独立的 .txt 文件，格式如图所示。
    Args:
        chunks_data (List[Dict[str, Any]]): 经过隐私过滤和分块后的数据列表。
                                          每个字典包含 'metadata' 和 'chunks' 键。
        output_sub_dir (str): 输出的子目录，用于区分不同的原始文件。
    """
    full_output_path = os.path.join(OUTPUT_DIR, output_sub_dir)
    os.makedirs(full_output_path, exist_ok=True) # 确保输出目录存在

    for i, unit in enumerate(chunks_data):
        # 构造文件名：例如 original_file_name_unit_001.txt
        output_filename = os.path.join(full_output_path, f"{output_sub_dir}_unit_{i+1:03d}.txt")
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            # 写入元数据
            for key, value in unit['metadata'].items():
                f.write(f"{key}: {value}\n")
            f.write("\n") # 空行分隔元数据和内容

            # 写入分块内容
            for chunk in unit['chunks']:
                f.write(f"Chunk [{chunk['Chunk_ID']}]:\n")
                if 'Content (FR)' in chunk and chunk['Content (FR)']:
                    f.write(f"Content (FR): {chunk['Content (FR)']}\n")
                if 'Content (EN)' in chunk and chunk['Content (EN)']:
                    f.write(f"Content (EN): {chunk['Content (EN)']}\n")
                f.write("\n") # 每个块之间也用空行分隔

        print(f"写入处理后的文件: {output_filename}")

# ==============================================================================
# C2D 范式入口函数：由 Cliox 平台调用
# ==============================================================================

def algorithm_main(input_file_path: str):
    """
    算法主入口函数，由 Cliox 平台调用。
    负责读取输入文件，调用隐私过滤和分块逻辑，并将结果输出。
    Args:
        input_file_path (str): 输入数据文件的路径。
    """
    print(f"正在处理文件: {input_file_path}")

    file_extension = os.path.splitext(input_file_path)[1].lower()
    original_file_name = os.path.basename(input_file_path)
    processed_units = [] # 存储处理后的独立输出单元
    input_data_identifier = original_file_name.replace('.', '_')

    try:
        if file_extension == '.json':
            with open(input_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            processed_units = filter_and_chunk_json_data(raw_data, original_file_name)
        elif file_extension == '.csv':
            with open(input_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                raw_data = [row for row in reader]
            processed_units = filter_and_chunk_csv_data(raw_data, original_file_name)
        else:
            print(f"不支持的文件格式: {file_extension}")
            return

        if processed_units:
            write_chunks_to_files(processed_units, input_data_identifier)
            print(f"成功处理并输出 {len(processed_units)} 个数据单元到 '{os.path.join(OUTPUT_DIR, input_data_identifier)}' 文件夹。")
        else:
            print(f"文件 {input_file_path} 未生成任何数据单元。")

    except Exception as e:
        print(f"处理文件 {input_file_path} 时发生错误: {e}")
        import traceback
        traceback.print_exc() # 打印详细错误信息，方便调试

# ==============================================================================
# 示例用法（在本地测试时可以直接运行此部分）
# ==============================================================================
if __name__ == "__main__":
    # 创建一个模拟的 JSON 输入文件进行测试
    sample_json_data = {
        "Official_Gazette": "Republic of Cameroon",
        "Date": "May 1994",
        "Page": 41,
        "Decrees": [
            {
                "Decree_No": "94/57",
                "Date": "16 March 1994",
                "Title_FR": "Décret portant nomination d’un chargé de mission au Cabinet Civil du Président de la République",
                "Title_EN": "Appointment of Chargé de Mission at the Civil Cabinet of the President of the Republic",
                "Appointee": {"Name": "Mme. Onambélénée Amvuma Rose", "Title": "professeur certifié", "Position": "Chargé de Mission", "Effective_Date": "16 March 1994"},
                "Articles": [
                    {"Article_No": 1, "Content_FR": "Est à compter de la date de signature du présent décret, nommée chargé de mission au Cabinet Civil du Président de la République : Mme. Onambélénée Amvuma Rose, professeur certifié.", "Content_EN": "Mrs. Onambélénée, née Rose Amvuma, High Schools Teacher, is, with effect from the date of signature of this decree, appointed Chargé de Mission at the Civil Cabinet of the President of the Republic."},
                    {"Article_No": 2, "Content_FR": "Le présent décret sera enregistré puis publié au Journal Officiel en français et en anglais.", "Content_EN": "This decree shall be registered and published in the Official Gazette in English and French."}
                ],
                "Signed_By": "Paul BIYA",
                "Location": "Yaoundé"
            },
            {
                "Decree_No": "94/58",
                "Date": "16 March 1994",
                "Title_FR": "Décret portant nomination d’un attaché au Cabinet Civil du Président de la République",
                "Title_EN": "Appointment of Attaché at the Civil Cabinet of the President of the Republic",
                "Appointee": {"Name": "M. Aboubakary Abdoulaye", "Title": "administrateur Civil", "Position": "Attaché", "Effective_Date": "16 March 1994"},
                "Articles": [
                    {"Article_No": 1, "Content_FR": "Est à compter de la date de signature du présent décret, nommé attaché au cabinet civil du Président de la République : M. Aboubakary Abdoulaye, administrateur Civil.", "Content_EN": "Mr. Aboubakary Abdoulaye, Administrative Officer, is, with effect from the date of signature of this decree, appointed Attaché at the Civil Cabinet of the President of the Republic."},
                    {"Article_No": 2, "Content_FR": "Le présent décret sera enregistré puis publié au Journal Officiel en fran\u00e7ais et en anglais.", "Content_EN": "This decree shall be registered and published in the Official Gazette in English and French."}
                ],
                "Signed_By": "Paul BIYA",
                "Location": "Yaoundé"
            }
        ],
        "Orders": [
            {
                "Order_No": "14/PR",
                "Date": "1 March 1994",
                "Title_FR": "Arrêté portant nomination d’un greffier en chef",
                "Title_EN": "Appointment",
                "Appointee": {"Name": "M. Kaptchouang Ngoupeyou Martin", "Previous_Role": "Chef de service des affaires administratives et financières au Parquet général de la cour d’appel de Garoua", "New_Role_FR": "greffier en chef du tribunal de première instance de Ka\u00e9lé et du tribunal de grande instance du Mayo Kani", "New_Role_EN": "Registrar-in-Chief of the Kaele Court to First Instance and the Mayo Kani High Court", "Replacement": "M. Kamaha Kamaha André-Marius"},
                "Articles": [
                    {"Article_No": 1, "Content_FR": "M. Kaptchouang Ngoupeyou Martin... est nommé greffier en chef...", "Content_EN": "Mr. Martin Kaptchouang Ngoupeyou... is hereby appointed Registrar-in-Chief..."},
                    {"Article_No": 2, "Content_FR": "L’intéressé aura droit aux avantages de toute nature prévus par la réglementation en vigueur.", "Content_EN": "Mr. Martin Kaptchouang Ngoupeyou shall be entitled to the various benefits provided for by the regulations in force."},
                    {"Article_No": 3, "Content_FR": "Le présent arrêté sera enregistré puis publié au Journal Officiel en fran\u00e7ais et en anglais.", "Content_EN": "This order shall be registered and published in the Official Gazette in English and French."}
                ],
                "Signed_By": "Paul BIYA",
                "Location": "Yaoundé"
            }
        ]
    }

    # 创建一个临时 JSON 文件进行测试
    temp_json_file = "sample_data_file.json" # 修改文件名以匹配Source格式
    with open(temp_json_file, 'w', encoding='utf-8') as f:
        json.dump(sample_json_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n模拟 JSON 文件已创建: {temp_json_file}")
    algorithm_main(temp_json_file)

    # 模拟 CSV 文件处理（这里需要你提供CSV样本以完成具体过滤逻辑）
    # 为了测试方便，我们创建一个简单的CSV文件
    temp_csv_file = "sample_csv_data.csv"
    sample_csv_data = [
        {'ID': '1', 'Name': 'Alice Wonderland', 'Date': '2023-01-15', 'Subject': 'Project Alpha Update', 'Text_Content': 'This document details the progress of Project Alpha, led by Alice Wonderland. Alice presented the findings to the board.'},
        {'ID': '2', 'Name': 'Bob Thebuilder', 'Date': '2023-02-20', 'Subject': 'New Infrastructure Plan', 'Text_Content': 'Bob Thebuilder designed the new system. Please contact Bob for more details.'}
    ]
    with open(temp_csv_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['ID', 'Name', 'Date', 'Subject', 'Text_Content']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_csv_data)
    
    print(f"\n模拟 CSV 文件已创建: {temp_csv_file}")
    algorithm_main(temp_csv_file)
    
    # 清理临时文件 (测试完毕后可以取消注释)
    # os.remove(temp_json_file)
    # os.remove(temp_csv_file)
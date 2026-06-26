import sys
import os

def filter_log_file(input_file, output_file):
    """
    从目标日志文件中提取 Kafka 消费者收到的 JSON 字符串。
    匹配标记为 "aZhangGuiLogKafkaConsumer onMessage,value: "
    """
    marker = "ZhangGuiLogKafkaConsumer onMessage,value:"
    count = 0
    
    print(f"开始处理文件: {input_file} ...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as fin, \
             open(output_file, 'w', encoding='utf-8') as fout:
            
            for line in fin:
                idx = line.find(marker)
                if idx != -1:
                    # 提取标记之后的全部字符串并去除首尾空白及换行符
                    json_str = line[idx + len(marker):].strip()
                    if json_str:
                        fout.write(json_str + '\n')
                        count += 1
                        
        print(f"提取完成！共提取 {count} 条 JSON 日志，已保存至: {output_file}")
        
    except Exception as e:
        print(f"处理文件时发生错误: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python filter_logs.py <输入日志文件> <输出JSON文件>")
        print("示例: python filter_logs.py process-management-2026-06-25-1.log output.json")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"错误: 找不到输入文件 '{input_file}'")
        sys.exit(1)
        
    filter_log_file(input_file, output_file)

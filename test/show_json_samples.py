import json

count_req = 0
count_res = 0

with open("output_filtered.json", "r", encoding="utf-8") as f:
    for line in f:
        try:
            data = json.loads(line.strip())
            loginfo = data.get("loginfo", "")
            if "Request={" in loginfo and count_req < 2:
                print("--- REQUEST ---")
                print(loginfo)
                count_req += 1
            if "Response={" in loginfo and count_res < 2:
                print("--- RESPONSE ---")
                print(loginfo)
                count_res += 1
                
            if count_req >= 2 and count_res >= 2:
                break
        except Exception as e:
            pass

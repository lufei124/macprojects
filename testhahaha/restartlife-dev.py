#!/usr/bin/env python3
"""
CSV 中 URL 域名批量替换工具。

支持将 https:// 或 http:// 后面的桶域名在以下三个之间互相替换：
  - s-project-neo
  - s-project-neo-dev
  - s-project-neo-test

交互流程：
  1. 用户输入「修改前的域名片段」→ 扫描所有 CSV 统计匹配数
  2. 用户输入「修改后的域名片段」
  3. 确认后执行替换（原地修改）
"""

import csv
import os
import re
import sys


ALLOWED_NAMES = {
    "s-project-neo",
    "s-project-neo-dev",
    "s-project-neo-test",
    "restartlife-dev",
}


def count_matches_in_csv(file_path, pattern, file_encoding="utf-8-sig"):
    """统计单个 CSV 文件中匹配 pattern 的次数，返回 (行数, 匹配次数)。"""
    total_rows = 0
    total_hits = 0
    try:
        with open(file_path, mode="r", encoding=file_encoding, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                total_rows += 1
                for cell in row:
                    total_hits += len(pattern.findall(cell))
    except (UnicodeDecodeError, PermissionError):
        pass
    return total_rows, total_hits


def replace_in_csv(file_path, pattern, new_text, file_encoding="utf-8-sig"):
    """
    原地替换单个 CSV 文件中所有匹配 pattern 的内容为 new_text。
    返回 (扫描行数, 替换次数)。
    """
    dir_name, base_name = os.path.split(file_path)
    if not dir_name:
        dir_name = "."
    temp_path = os.path.join(dir_name, f"._tmp_{base_name}")

    row_count = 0
    replace_count = 0

    with open(file_path, mode="r", encoding=file_encoding, newline="") as infile, \
         open(temp_path, mode="w", encoding=file_encoding, newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            row_count += 1
            new_row = []
            for cell in row:
                new_cell, n = pattern.subn(new_text, cell)
                replace_count += n
                new_row.append(new_cell)
            writer.writerow(new_row)

    os.replace(temp_path, file_path)
    return row_count, replace_count


def collect_csv_files(root_dir):
    """递归收集目录下所有 .csv 文件路径。"""
    result = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith(".csv"):
                result.append(os.path.join(dirpath, fn))
    result.sort()
    return result


def ask_name(prompt):
    """交互式输入，必须是允许的三个名称之一。"""
    allowed = sorted(ALLOWED_NAMES)
    while True:
        print(f"\n{prompt}")
        print(f"可选: {', '.join(allowed)}")
        name = input("请输入: ").strip()
        if name not in ALLOWED_NAMES:
            print(f"❌ 错误：'{name}' 不在允许范围内，只能填以下三个之一：")
            print(f"   {', '.join(allowed)}")
            continue
        return name


def build_url_pattern(name):
    """
    构建正则，匹配 https:// 或 http:// 后面紧跟的桶域名片段。
    例如 name="s-project-neo-test" 会匹配：
      https://s-project-neo-test.obs.cn-east-3...
      http://s-project-neo-test.obs.cn-east-3...
    """
    escaped = re.escape(name)
    return re.compile(r"(?<=https://)" + escaped + r"(?=\.)" + r"|" + r"(?<=http://)" + escaped + r"(?=\.)")


def main():
    root_dir = "."
    csv_files = collect_csv_files(root_dir)
    if not csv_files:
        print(f"当前目录 {os.path.abspath(root_dir)} 下没有找到任何 .csv 文件。")
        return

    print(f"扫描目录: {os.path.abspath(root_dir)}")
    print(f"找到 {len(csv_files)} 个 CSV 文件")

    # 第一步：输入修改前的内容
    old_name = ask_name("【第一步】请输入要替换的原域名（修改前）：")

    # 扫描统计
    pattern = build_url_pattern(old_name)
    print(f"\n正在扫描所有 CSV 文件中 URL 里的 \"{old_name}\" ...")

    total_files_hit = 0
    total_match_count = 0
    file_details = []

    for fp in csv_files:
        rows, hits = count_matches_in_csv(fp, pattern)
        if hits > 0:
            total_files_hit += 1
            total_match_count += hits
            file_details.append((fp, rows, hits))

    print(f"\n扫描结果：")
    print(f"  涉及文件数: {total_files_hit} / {len(csv_files)}")
    print(f"  匹配总次数: {total_match_count}")

    if total_match_count == 0:
        print(f"\n没有在任何 CSV 的 URL 中找到 \"{old_name}\"，无需替换。")
        return

    for fp, rows, hits in file_details:
        print(f"  - {fp}  ({hits} 处匹配, {rows} 行)")

    # 第二步：输入修改后的内容
    while True:
        new_name = ask_name("【第二步】请输入替换后的域名（修改后）：")
        if new_name == old_name:
            print(f"❌ 修改后不能和修改前相同（{old_name}）")
            continue
        break

    # 确认
    print(f"\n{'=' * 55}")
    print(f"即将执行替换：")
    print(f"  修改前: {old_name}")
    print(f"  修改后: {new_name}")
    print(f"  匹配数: {total_match_count} 处（{total_files_hit} 个文件）")
    print(f"  示例:   https://{old_name}.obs...  →  https://{new_name}.obs...")
    print(f"{'=' * 55}")

    confirm = input("\n确认替换？(y/n): ").strip().lower()
    if confirm != "y":
        print("已取消。")
        return

    # 执行替换
    new_text = new_name
    grand_rows = 0
    grand_replaced = 0

    for fp, _, _ in file_details:
        rows, replaced = replace_in_csv(fp, pattern, new_text)
        grand_rows += rows
        grand_replaced += replaced
        print(f"  ✅ {fp}  替换 {replaced} 处")

    print(f"\n✅ 全部完成！")
    print(f"  共扫描 {grand_rows} 行")
    print(f"  共替换 {grand_replaced} 处")
    print(f"  URL 域名已从 {old_name} 改为 {new_name}")


if __name__ == "__main__":
    main()

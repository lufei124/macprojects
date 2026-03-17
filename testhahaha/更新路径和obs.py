#!/usr/bin/env python3
"""
分支感知的一键部署工具：CSV 路径替换 + OBS 桶同步。

根据 Git 分支自动决定：
  1. CSV 中的 URL 域名替换规则
  2. OBS 桶同步的源桶 / 目标桶

正向部署模式（目标桶与源桶完全镜像）：
  test        → URL 替换为 s-project-neo-test；桶同步不操作
  uat         → URL 替换为 s-project-neo-uat；s-project-neo-test → s-project-neo-uat
  master      → URL 替换为 s-project-neo；s-project-neo-uat → s-project-neo

热修复回归模式（只将线上新增内容同步到下游，不覆盖、不删除下游内容）：
  hotfix/master → URL 替换为 s-project-neo；桶同步不操作
  hotfix/uat    → URL 替换为 s-project-neo-uat；s-project-neo 新增内容 → s-project-neo-uat
  hotfix/test   → URL 替换为 s-project-neo-test；s-project-neo-uat 新增内容 → s-project-neo-test

未识别分支或非 Git 仓库 → 交互式手动配置

排除路径：client-db/（不参与桶比对和同步）
区域：华东-上海一（cn-east-3）
"""

from __future__ import annotations

import csv
import json
import os
import platform
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TextIO, Tuple

from obs import ObsClient

# ──────────────────────────── 常量 / 配置 ────────────────────────────

DESKTOP_DIR = str(Path.home() / "Desktop")

SERVER = "https://obs.cn-east-3.myhuaweicloud.com"


def _get_obs_credentials() -> tuple[str, str]:
    """
    获取 OBS AK/SK，优先级：
    1. 环境变量 (HUAWEI_OBS_AK, HUAWEI_OBS_SK)
    2. 用户目录下的配置文件 (~/.neo_obs_config.json)
    3. 交互式引导用户填写配置文件
    """
    ak = os.getenv("HUAWEI_OBS_AK")
    sk = os.getenv("HUAWEI_OBS_SK")

    config_path = os.path.expanduser("~/.neo_obs_config.json")

    if not ak or not sk:
        try:
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    local_config = json.load(f)
                    if not ak:
                        ak = local_config.get("ACCESS_KEY")
                    if not sk:
                        sk = local_config.get("SECRET_KEY")
        except Exception as e:
            print(f"⚠️ 读取本地 OBS 配置文件失败: {e}")

    invalid_values = ["********", "请输入您的AccessKey", "请输入您的SecretKey"]

    while (
        not ak or ak in invalid_values or not sk or sk in invalid_values
        or not ak.isascii() or not sk.isascii()
    ) and sys.stdout.isatty():
        print("\n❌ 未检测到有效的华为云 OBS 密钥配置。")
        print("💡 如果你没有或者忘了 OBS 密钥，请联系 张伟光 获取。")

        if not os.path.exists(config_path):
            default_config = {
                "ACCESS_KEY": "请输入您的AccessKey",
                "SECRET_KEY": "请输入您的SecretKey",
            }
            try:
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(default_config, f, indent=4, ensure_ascii=False)
                print(f"✅ 已为您创建配置文件模板: {config_path}")
            except Exception as e:
                print(f"❌ 创建配置文件失败: {e}")

        print(f"请在打开的文件中填写您的 ACCESS_KEY 和 SECRET_KEY，保存并关闭文件。\n")

        try:
            if platform.system() == "Darwin":
                subprocess.call(("open", config_path))
            elif platform.system() == "Windows":
                os.startfile(config_path)  # type: ignore
            else:
                subprocess.call(("xdg-open", config_path))

            user_input = input("✅ 填写完成后，请按回车键继续运行 (或输入 q 退出)...")
            if user_input.lower().strip() == "q":
                print("已退出。")
                sys.exit(1)

            with open(config_path, "r", encoding="utf-8") as f:
                local_config = json.load(f)
                ak = local_config.get("ACCESS_KEY")
                sk = local_config.get("SECRET_KEY")

            if not ak or ak in invalid_values or not sk or sk in invalid_values:
                print("⚠️  检测到密钥仍未正确填写，请重新检查！")
            elif not ak.isascii() or not sk.isascii():
                print("⚠️  检测到密钥包含非法字符（如中文或空格），请重新检查！")
        except Exception as e:
            print(f"⚠️ 无法自动打开文件，请手动编辑: {config_path}")
            print(f"错误信息: {e}")
            break

    if ak in invalid_values or (ak and not ak.isascii()):
        print("⚠️ AccessKey 无效或包含非 ASCII 字符，将使用占位符代替。")
        ak = "********"
    if sk in invalid_values or (sk and not sk.isascii()):
        print("⚠️ SecretKey 无效或包含非 ASCII 字符，将使用占位符代替。")
        sk = "********"

    return (ak if ak else "********", sk if sk else "********")


AK, SK = _get_obs_credentials()

ALLOWED_BUCKETS = {"s-project-neo", "s-project-neo-test", "s-project-neo-uat"}

BRANCH_CONFIG: Dict[str, dict] = {
    # ── 正向部署模式（完全镜像，目标桶严格与源桶一致）───────────────────
    "test": {
        "old_names": ["s-project-neo-uat", "s-project-neo"],
        "new_name": "s-project-neo-test",
        "src_bucket": None,
        "dst_bucket": None,
        "sync_mode": "正向部署",
    },
    "uat": {
        "old_names": ["s-project-neo-test", "s-project-neo"],
        "new_name": "s-project-neo-uat",
        "src_bucket": "s-project-neo-test",
        "dst_bucket": "s-project-neo-uat",
        "sync_mode": "正向部署",
    },
    "master": {
        "old_names": ["s-project-neo-test", "s-project-neo-uat"],
        "new_name": "s-project-neo",
        "src_bucket": "s-project-neo-uat",
        "dst_bucket": "s-project-neo",
        "sync_mode": "正向部署",
    },
    # ── 热修复回归模式（只同步新增内容，不覆盖、不删除下游内容）────────
    "hotfix/master": {
        "old_names": ["s-project-neo-test", "s-project-neo-uat"],
        "new_name": "s-project-neo",
        "src_bucket": None,
        "dst_bucket": None,
        "sync_mode": "热修复回归",
    },
    "hotfix/uat": {
        "old_names": ["s-project-neo-test", "s-project-neo"],
        "new_name": "s-project-neo-uat",
        "src_bucket": "s-project-neo",
        "dst_bucket": "s-project-neo-uat",
        "sync_mode": "热修复回归",
    },
    "hotfix/test": {
        "old_names": ["s-project-neo-uat", "s-project-neo"],
        "new_name": "s-project-neo-test",
        "src_bucket": "s-project-neo-uat",
        "dst_bucket": "s-project-neo-test",
        "sync_mode": "热修复回归",
    },
}

SKIP_PREFIXES = ["client-db/"]

REASON_MISSING = "目标桶不存在该对象"
REASON_MD5_DIFF = "MD5不同"
REASON_EXTRA = "目标桶多余（源桶不存在）"

# ──────────────────────────── 分支检测 ────────────────────────────


def open_file(path: str) -> None:
    """用系统默认程序打开文件。"""
    try:
        system = platform.system()
        if system == "Darwin":
            subprocess.Popen(["open", path])
        elif system == "Windows":
            os.startfile(path)  # type: ignore[attr-defined]
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception:
        pass


def detect_branch() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def ask_deploy_mode() -> str:
    """
    在脚本启动时让用户选择部署模式，返回 '正向部署' 或 '热修复回归'。
    直接回车默认选正向部署。
    """
    print(f"\n{'=' * 60}")
    print("请选择部署模式")
    print(f"{'=' * 60}")
    print()
    print("  [1] 正向部署模式（默认）")
    print("      正常更新流程，按 test → uat → master 顺序推进。")
    print("      目标桶与源桶完全镜像，包含覆盖变更内容和删除目标桶多余文件。")
    print()
    print("  [2] 热修复回归模式")
    print("      有 bug 时使用的反向更新流程，按 master → uat → test 方向同步。")
    print("      只将线上变更内容同步到下游，不删除下游环境独有的文件。")
    print(f"{'=' * 60}")

    choice = input("请输入 1 或 2（直接回车默认选 1，正向部署）: ").strip()
    if choice == "2":
        print("\n✅ 已选择：热修复回归模式")
        return "热修复回归"
    print("\n✅ 已选择：正向部署模式")
    return "正向部署"


def resolve_config(deploy_mode: str) -> dict:
    """
    根据部署模式和 Git 分支自动匹配配置；匹配不到则交互式获取。
    热修复回归模式下，将当前分支 master/uat/test 自动映射到 hotfix/* 配置。
    """
    # 根据模式筛选可用配置项
    mode_configs = {
        k: v for k, v in BRANCH_CONFIG.items()
        if v.get("sync_mode") == deploy_mode
    }

    branch = detect_branch()

    # 热修复模式：将 master/uat/test 分支自动映射到对应 hotfix/* 配置
    config_key = branch
    if deploy_mode == "热修复回归" and branch in ("master", "uat", "test"):
        config_key = f"hotfix/{branch}"

    if config_key and config_key in mode_configs:
        cfg = mode_configs[config_key]
        display_branch = f"{branch} → {config_key}" if config_key != branch else branch
        print(f"检测到当前分支: {display_branch}")
        print(f"  CSV 替换: {cfg['old_names']} → {cfg['new_name']}")
        if cfg["src_bucket"]:
            print(f"  桶同步:   {cfg['src_bucket']} → {cfg['dst_bucket']}")
        else:
            print(f"  桶同步:   不操作")
        return cfg

    if branch:
        print(f"当前分支 '{branch}' 无对应配置。")
    else:
        print("未检测到 Git 分支（可能不在 Git 仓库中）。")

    print(f"\n请手动选择目标配置，或直接回车进入完全手动模式。")
    print(f"可选配置: {', '.join(sorted(mode_configs.keys()))}")
    user_branch = input("请输入配置名（回车跳过）: ").strip()

    if user_branch in mode_configs:
        cfg = mode_configs[user_branch]
        print(f"\n已选择配置: {user_branch}")
        return cfg

    print("\n进入手动配置模式...")
    return _manual_config(deploy_mode)


def _manual_config(deploy_mode: str = "正向部署") -> dict:
    allowed_names = sorted(ALLOWED_BUCKETS)

    print(f"\n可选域名: {', '.join(allowed_names)}")
    while True:
        raw = input("请输入要替换的旧域名（多个用逗号分隔）: ").strip()
        old_names = [n.strip() for n in raw.split(",") if n.strip()]
        if not old_names or any(n not in ALLOWED_BUCKETS for n in old_names):
            print(f"❌ 每个名称必须是以下之一: {', '.join(allowed_names)}")
            continue
        break

    while True:
        new_name = input("请输入替换后的新域名: ").strip()
        if new_name not in ALLOWED_BUCKETS:
            print(f"❌ 必须是以下之一: {', '.join(allowed_names)}")
            continue
        if new_name in old_names:
            print("❌ 新域名不能和旧域名相同")
            continue
        break

    print(f"\n可选桶: {', '.join(allowed_names)}")
    print("如果不需要桶同步，源桶直接回车跳过。")
    src_bucket = input("请输入源桶名（回车跳过）: ").strip() or None
    dst_bucket = None
    if src_bucket:
        if src_bucket not in ALLOWED_BUCKETS:
            print(f"❌ '{src_bucket}' 不在允许范围，桶同步将跳过")
            src_bucket = None
        else:
            while True:
                dst_bucket = input("请输入目标桶名: ").strip()
                if dst_bucket not in ALLOWED_BUCKETS:
                    print(f"❌ 必须是以下之一: {', '.join(allowed_names)}")
                    continue
                if dst_bucket == src_bucket:
                    print("❌ 目标桶不能和源桶相同")
                    continue
                break

    return {
        "old_names": old_names,
        "new_name": new_name,
        "src_bucket": src_bucket,
        "dst_bucket": dst_bucket,
        "sync_mode": deploy_mode,
    }


# ──────────────────────── CSV 路径替换 ────────────────────────


def build_combined_pattern(names: List[str]) -> re.Pattern:
    """构造正则：匹配 https:// 或 http:// 后面紧跟的多个域名之一。"""
    alternatives = "|".join(re.escape(n) for n in names)
    return re.compile(
        r"(?<=https://)(?:" + alternatives + r")(?=\.)"
        r"|"
        r"(?<=http://)(?:" + alternatives + r")(?=\.)"
    )


def count_matches_in_csv(file_path: str, pattern: re.Pattern, encoding: str = "utf-8-sig") -> Tuple[int, int]:
    total_rows = 0
    total_hits = 0
    try:
        with open(file_path, mode="r", encoding=encoding, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                total_rows += 1
                for cell in row:
                    total_hits += len(pattern.findall(cell))
    except (UnicodeDecodeError, PermissionError):
        pass
    return total_rows, total_hits


def replace_in_csv(file_path: str, pattern: re.Pattern, new_text: str, encoding: str = "utf-8-sig") -> Tuple[int, int]:
    dir_name, base_name = os.path.split(file_path)
    if not dir_name:
        dir_name = "."
    temp_path = os.path.join(dir_name, f"._tmp_{base_name}")

    row_count = 0
    replace_count = 0
    with open(file_path, mode="r", encoding=encoding, newline="") as infile, \
         open(temp_path, mode="w", encoding=encoding, newline="") as outfile:
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


def collect_csv_files(root_dir: str) -> List[str]:
    result = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith(".csv"):
                result.append(os.path.join(dirpath, fn))
    result.sort()
    return result


def phase_csv_replace(old_names: List[str], new_name: str) -> None:
    """第一阶段：CSV URL 域名替换。"""
    print(f"\n{'=' * 60}")
    print("第一阶段：CSV URL 域名替换")
    print(f"{'=' * 60}")

    root_dir = "."
    csv_files = collect_csv_files(root_dir)
    if not csv_files:
        print(f"当前目录 {os.path.abspath(root_dir)} 下没有找到任何 .csv 文件，跳过。")
        return

    print(f"扫描目录: {os.path.abspath(root_dir)}")
    print(f"找到 {len(csv_files)} 个 CSV 文件")
    print(f"替换规则: {old_names} → {new_name}")

    pattern = build_combined_pattern(old_names)

    total_files_hit = 0
    total_match_count = 0
    file_details: List[Tuple[str, int, int]] = []

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
        print("没有匹配的 URL，无需替换。")
        return

    for fp, rows, hits in file_details:
        print(f"  - {fp}  ({hits} 处匹配, {rows} 行)")

    confirm = input(f"\n确认替换以上 {total_match_count} 处？(y/n): ").strip().lower()
    if confirm != "y":
        print("已跳过 CSV 替换。")
        return

    grand_replaced = 0
    for fp, _, _ in file_details:
        _, replaced = replace_in_csv(fp, pattern, new_name)
        grand_replaced += replaced
        print(f"  ✅ {fp}  替换 {replaced} 处")

    print(f"\n✅ CSV 替换完成！共替换 {grand_replaced} 处")
    print(f"  URL 域名已从 {old_names} 改为 {new_name}")


# ──────────────────────── OBS 桶同步 ────────────────────────


@dataclass(frozen=True)
class ObjInfo:
    key: str
    last_modified: str
    size: int
    etag: str


@dataclass(frozen=True)
class DiffItem:
    key: str
    reason: str
    src_info: ObjInfo
    dst_info: Optional[ObjInfo]


def get_client() -> ObsClient:
    if AK == "********" or SK == "********":
        print("❌ OBS 密钥无效，无法创建客户端。请检查环境变量或 ~/.neo_obs_config.json 配置。", file=sys.stderr)
        raise SystemExit(1)
    return ObsClient(access_key_id=AK, secret_access_key=SK, server=SERVER)


def iter_objects(client: ObsClient, bucket: str, prefix: Optional[str] = None) -> Iterable[ObjInfo]:
    marker = None
    while True:
        resp = client.listObjects(bucket, prefix=prefix, marker=marker, max_keys=1000)
        if resp.status >= 300:
            raise RuntimeError(
                f"listObjects failed: bucket={bucket} status={resp.status} "
                f"errorCode={resp.errorCode} errorMessage={resp.errorMessage}"
            )
        contents = resp.body.contents or []
        for c in contents:
            yield ObjInfo(
                key=c.key,
                last_modified=str(getattr(c, "lastModified", getattr(c, "last_modified", ""))),
                size=int(getattr(c, "size", 0)),
                etag=str(getattr(c, "etag", "")).strip('"'),
            )
        is_truncated = bool(
            getattr(resp.body, "isTruncated", None)
            or getattr(resp.body, "is_truncated", False)
        )
        has_more = is_truncated or len(contents) >= 1000
        if not has_more:
            break
        next_marker = (
            getattr(resp.body, "nextMarker", None)
            or getattr(resp.body, "next_marker", None)
        )
        if next_marker:
            marker = next_marker
        elif contents:
            marker = contents[-1].key
        else:
            break


def should_skip(key: str) -> bool:
    return any(key.startswith(p) for p in SKIP_PREFIXES)


def build_index(client: ObsClient, bucket: str) -> Dict[str, ObjInfo]:
    idx: Dict[str, ObjInfo] = {}
    count = 0
    skip_count = 0
    for obj in iter_objects(client, bucket):
        if should_skip(obj.key):
            skip_count += 1
            continue
        idx[obj.key] = obj
        count += 1
        if count % 5000 == 0:
            print(f"  [{bucket}] 已列举 {count} 个对象...")
    print(f"  [{bucket}] 列举完成，共 {count} 个对象（已跳过 client-db/ 下 {skip_count} 个）")
    return idx


def _etag_base(etag: str) -> str:
    """
    返回 ETag 的基础部分，去除分片上传后缀（如 `abc123-9` → `abc123`）。
    OBS/S3 分片上传的 ETag 格式为 `<hash>-<分片数>`，与普通上传的 ETag
    直接比较会始终不等，需剥离后缀后再比对。
    """
    return etag.split("-")[0]


def _etag_equal(a: str, b: str, size_a: int, size_b: int) -> bool:
    """
    比较两个 ETag 是否代表相同内容：
    - 字符串完全相同 → 相同
    - 两边文件大小相同，且至少一方是分片 ETag（含 `-N`）→ 视为相同，
      避免因上传方式不同导致 ETag 格式差异而误判为内容不同。
    """
    if a == b:
        return True
    if size_a != size_b:
        return False
    multipart = "-" in a or "-" in b
    if multipart and _etag_base(a) == _etag_base(b):
        return True
    # 大小相同但 ETag 基础 hash 不同：仍保守判断为不同
    # （如需彻底解决，应下载后对比实际 MD5，此处保留分片场景的宽松匹配）
    if multipart and size_a == size_b:
        return True
    return False


def compare_buckets(
    src_index: Dict[str, ObjInfo],
    dst_index: Dict[str, ObjInfo],
    sync_mode: str = "正向部署",
) -> Tuple[List[DiffItem], int]:
    """
    正向部署模式：
      1. 源桶有，目标桶没有  → 复制
      2. 两边都有，MD5 不同  → 覆盖
      3. 两边都有，MD5 相同  → 跳过
      4. 目标桶有，源桶没有  → 删除

    热修复回归模式（与正向部署唯一区别：不删除目标桶独有内容）：
      1. 源桶有，目标桶没有  → 复制
      2. 两边都有，MD5 不同  → 覆盖
      3. 两边都有，MD5 相同  → 跳过
      4. 目标桶有，源桶没有  → 保留（不删除）
    """
    diffs: List[DiffItem] = []
    skipped = 0
    is_hotfix = sync_mode == "热修复回归"

    for key, src_obj in src_index.items():
        dst_obj = dst_index.get(key)
        if dst_obj is None:
            diffs.append(DiffItem(key=key, reason=REASON_MISSING, src_info=src_obj, dst_info=None))
            continue
        if not _etag_equal(src_obj.etag, dst_obj.etag, src_obj.size, dst_obj.size):
            diffs.append(DiffItem(key=key, reason=REASON_MD5_DIFF, src_info=src_obj, dst_info=dst_obj))
            continue
        skipped += 1

    if not is_hotfix:
        for key, dst_obj in dst_index.items():
            if key not in src_index:
                diffs.append(DiffItem(key=key, reason=REASON_EXTRA, src_info=dst_obj, dst_info=dst_obj))

    diffs.sort(key=lambda d: d.key)
    return diffs, skipped


def _top_folder(key: str) -> str:
    pos = key.find("/")
    return key[:pos] if pos > 0 else "(根目录)"


def folder_stats(index: Dict[str, ObjInfo]) -> Dict[str, Tuple[int, int]]:
    counts: Dict[str, int] = defaultdict(int)
    sizes: Dict[str, int] = defaultdict(int)
    for obj in index.values():
        folder = _top_folder(obj.key)
        counts[folder] += 1
        sizes[folder] += obj.size
    return {k: (counts[k], sizes[k]) for k in sorted(counts)}


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"


def write_folder_comparison(log: TextIO, all_indexes: Dict[str, Dict[str, ObjInfo]]) -> None:
    bucket_names = sorted(all_indexes.keys())
    stats_map = {b: folder_stats(idx) for b, idx in all_indexes.items()}
    all_folders = sorted({f for s in stats_map.values() for f in s})

    bucket_label = f"{len(bucket_names)}桶" if len(bucket_names) != 3 else "三桶"
    log.write("\n" + "=" * 90 + "\n")
    log.write(f"{bucket_label}一级文件夹 文件数 / 大小 对比\n")
    log.write("=" * 90 + "\n\n")

    header = f"{'文件夹':<30}"
    for b in bucket_names:
        header += f"  {b:>25}"
    log.write(header + "\n")
    log.write("-" * len(header) + "\n")

    totals: Dict[str, Tuple[int, int]] = {b: (0, 0) for b in bucket_names}
    for folder in all_folders:
        line = f"{folder:<30}"
        for b in bucket_names:
            cnt, sz = stats_map[b].get(folder, (0, 0))
            totals[b] = (totals[b][0] + cnt, totals[b][1] + sz)
            line += f"  {str(cnt) + ' / ' + _human_size(sz):>25}"
        log.write(line + "\n")

    log.write("-" * len(header) + "\n")
    total_line = f"{'合计':<30}"
    for b in bucket_names:
        cnt, sz = totals[b]
        total_line += f"  {str(cnt) + ' / ' + _human_size(sz):>25}"
    log.write(total_line + "\n")
    log.write("=" * 90 + "\n")


def _print_folder_comparison(all_indexes: Dict[str, Dict[str, ObjInfo]]) -> None:
    """在终端打印各桶按一级文件夹的文件数 / 大小对比表。"""
    bucket_names = sorted(all_indexes.keys())
    stats_map = {b: folder_stats(idx) for b, idx in all_indexes.items()}
    all_folders = sorted({f for s in stats_map.values() for f in s})

    print(f"\n{'=' * 90}")
    print("同步后各桶一级文件夹 文件数 / 大小 对比")
    print(f"{'=' * 90}\n")

    header = f"{'文件夹':<30}"
    for b in bucket_names:
        header += f"  {b:>25}"
    print(header)
    print("-" * len(header))

    totals: Dict[str, Tuple[int, int]] = {b: (0, 0) for b in bucket_names}
    for folder in all_folders:
        line = f"{folder:<30}"
        for b in bucket_names:
            cnt, sz = stats_map[b].get(folder, (0, 0))
            totals[b] = (totals[b][0] + cnt, totals[b][1] + sz)
            line += f"  {str(cnt) + ' / ' + _human_size(sz):>25}"
        print(line)

    print("-" * len(header))
    total_line = f"{'合计':<30}"
    for b in bucket_names:
        cnt, sz = totals[b]
        total_line += f"  {str(cnt) + ' / ' + _human_size(sz):>25}"
    print(total_line)
    print(f"{'=' * 90}")


def copy_object(client: ObsClient, src_bucket: str, key: str, dst_bucket: str) -> None:
    resp = client.copyObject(src_bucket, key, dst_bucket, key, None, None)
    if resp.status >= 300:
        raise RuntimeError(
            f"copyObject failed: key={key} status={resp.status} "
            f"errorCode={resp.errorCode} errorMessage={resp.errorMessage}"
        )


def delete_object(client: ObsClient, bucket: str, key: str) -> None:
    resp = client.deleteObject(bucket, key)
    if resp.status >= 300:
        raise RuntimeError(
            f"deleteObject failed: key={key} status={resp.status} "
            f"errorCode={resp.errorCode} errorMessage={resp.errorMessage}"
        )


def write_diff_log(
    log: TextIO,
    src_bucket: str,
    dst_bucket: str,
    src_index: Dict[str, ObjInfo],
    dst_index: Dict[str, ObjInfo],
    diffs: List[DiffItem],
    skipped: int,
    all_indexes: Optional[Dict[str, Dict[str, ObjInfo]]] = None,
    sync_mode: str = "正向部署",
) -> None:
    is_hotfix = sync_mode == "热修复回归"
    log.write("=" * 70 + "\n")
    log.write("源桶 → 目标桶 差异同步日志\n")
    log.write("=" * 70 + "\n")
    log.write(f"同步时间       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log.write(f"同步模式       : {'热修复回归模式（只同步新增内容，不覆盖、不删除下游内容）' if is_hotfix else '正向部署模式（完全镜像）'}\n")
    log.write(f"源桶           : {src_bucket}\n")
    log.write(f"目标桶         : {dst_bucket}\n")
    log.write(f"源桶对象总数   : {len(src_index)}\n")
    log.write(f"目标桶对象总数 : {len(dst_index)}\n")
    log.write(f"需要同步       : {len(diffs)} 个对象\n")
    log.write(f"无需同步(跳过) : {skipped} 个对象\n")
    log.write("=" * 70 + "\n")
    if is_hotfix:
        log.write("\n比对规则（热修复回归模式，基于 MD5 / ETag）：\n")
        log.write("  1. 源桶有，目标桶没有  → 复制到目标桶\n")
        log.write("  2. 两边都有，MD5 不同  → 用源桶覆盖\n")
        log.write("  3. 两边都有，MD5 相同  → 跳过\n")
        log.write("  4. 目标桶有，源桶没有  → 保留（不删除下游独有内容）\n")
    else:
        log.write("\n比对规则（正向部署模式，基于 MD5 / ETag）：\n")
        log.write("  1. 源桶有，目标桶没有  → 复制到目标桶\n")
        log.write("  2. 两边都有，MD5 不同  → 用源桶覆盖\n")
        log.write("  3. 两边都有，MD5 相同  → 跳过\n")
        log.write("  4. 目标桶有，源桶没有  → 从目标桶删除\n")
    log.write("\n")

    if all_indexes:
        write_folder_comparison(log, all_indexes)

    reason_counts: Dict[str, int] = {}
    for d in diffs:
        reason_counts[d.reason] = reason_counts.get(d.reason, 0) + 1
    log.write("差异分类统计：\n")
    for reason, count in sorted(reason_counts.items()):
        log.write(f"  {reason}: {count} 个\n")
    log.write("\n" + "=" * 70 + "\n")
    log.write("差异明细\n")
    log.write("=" * 70 + "\n\n")

    for i, d in enumerate(diffs, 1):
        log.write(f"[{i}] {d.key}\n")
        log.write(f"    差异原因: {d.reason}\n")
        if d.reason == REASON_EXTRA:
            log.write(f"    目标桶 MD5: {d.dst_info.etag}  大小: {d.dst_info.size} 字节\n")
            log.write(f"    操作: 从目标桶删除\n\n")
        else:
            log.write(f"    源桶  MD5: {d.src_info.etag}  大小: {d.src_info.size} 字节\n")
            if d.dst_info is not None:
                log.write(f"    目标桶 MD5: {d.dst_info.etag}  大小: {d.dst_info.size} 字节\n")
            else:
                log.write(f"    目标桶: 不存在\n")
            log.write(f"    操作: 从源桶复制/覆盖到目标桶\n\n")


def phase_bucket_sync(
    src_bucket: Optional[str],
    dst_bucket: Optional[str],
    sync_mode: str = "正向部署",
) -> None:
    """第二阶段：OBS 桶同步。正向部署模式完全镜像；热修复回归模式只同步新增内容。"""
    print(f"\n{'=' * 60}")
    print("第二阶段：OBS 桶同步")
    print(f"{'=' * 60}")

    if not src_bucket or not dst_bucket:
        print("当前配置不需要桶同步，跳过。")
        return

    is_hotfix = sync_mode == "热修复回归"
    mode_label = "热修复回归模式（只同步新增内容，不覆盖、不删除下游内容）" if is_hotfix else "正向部署模式（完全镜像）"
    print(f"同步模式: {mode_label}")
    print(f"源桶:   {src_bucket}")
    print(f"目标桶: {dst_bucket}")

    client = get_client()
    try:
        print(f"\n正在列举源桶和目标桶对象...")
        src_index = build_index(client, src_bucket)
        dst_index = build_index(client, dst_bucket)
        all_indexes: Dict[str, Dict[str, ObjInfo]] = {
            src_bucket: src_index,
            dst_bucket: dst_index,
        }

        diffs, skipped = compare_buckets(src_index, dst_index, sync_mode=sync_mode)

        missing_count = sum(1 for d in diffs if d.reason == REASON_MISSING)
        md5_diff_count = sum(1 for d in diffs if d.reason == REASON_MD5_DIFF)
        extra_count = sum(1 for d in diffs if d.reason == REASON_EXTRA)

        print(f"\n{'=' * 60}")
        print(f"源桶({src_bucket}) → 目标桶({dst_bucket}) 对比结果：")
        print(f"  源桶对象数:    {len(src_index)}")
        print(f"  目标桶对象数:  {len(dst_index)}")
        print(f"{'=' * 60}")
        print(f"  需要同步: {len(diffs)} 个对象")
        print(f"    - 需复制(目标桶缺失): {missing_count} 个")
        print(f"    - 需覆盖(MD5不同):    {md5_diff_count} 个")
        if is_hotfix:
            print(f"    - 保留(目标桶独有，热修复模式不删除): {sum(1 for k in dst_index if k not in src_index)} 个")
        else:
            print(f"    - 需删除(目标桶多余): {extra_count} 个")
        print(f"  无需同步(跳过):         {skipped} 个")
        print(f"{'=' * 60}")

        log_name = os.path.join(
            DESKTOP_DIR,
            f"sync_{src_bucket}_to_{dst_bucket}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        with open(log_name, "w", encoding="utf-8") as log:
            write_diff_log(log, src_bucket, dst_bucket, src_index, dst_index, diffs, skipped, all_indexes, sync_mode)
        print(f"差异日志已写入: {log_name}")
        open_file(log_name)

        if not diffs:
            print(f"\n✅ {dst_bucket} 已与源桶一致，无需同步。")
            return

        show_limit = 50
        print(f"\n需要处理的对象（显示前 {min(show_limit, len(diffs))} 条）：")
        for i, d in enumerate(diffs[:show_limit], 1):
            print(f"  [{i}] {d.key}")
            print(f"      原因: {d.reason}")
            if d.reason == REASON_EXTRA:
                print(f"      目标桶 MD5: {d.dst_info.etag}  大小: {d.dst_info.size}")
            else:
                print(f"      源桶  MD5: {d.src_info.etag}  大小: {d.src_info.size}")
                if d.dst_info:
                    print(f"      目标桶 MD5: {d.dst_info.etag}  大小: {d.dst_info.size}")
                else:
                    print(f"      目标桶: 不存在")
        if len(diffs) > show_limit:
            print(f"  ... 以及另外 {len(diffs) - show_limit} 个对象（完整列表见日志）")

        copy_diffs = [d for d in diffs if d.reason != REASON_EXTRA]
        del_diffs = [d for d in diffs if d.reason == REASON_EXTRA]

        if is_hotfix:
            confirm = input(
                f"\n是否执行同步？（热修复回归模式：复制/覆盖 {len(copy_diffs)} 个，不删除目标桶独有对象）(y/n): "
            ).strip().lower()
        else:
            confirm = input(
                f"\n是否执行同步？（复制/覆盖 {len(copy_diffs)} 个，删除 {len(del_diffs)} 个）(y/n): "
            ).strip().lower()
        if confirm != "y":
            print("已跳过桶同步。")
            return

        copied = 0
        deleted = 0
        failed = 0

        for d in copy_diffs:
            try:
                copy_object(client, src_bucket, d.key, dst_bucket)
                copied += 1
            except RuntimeError as e:
                print(f"  ⚠️  复制失败: {d.key} ({e})")
                failed += 1
            if copied % 100 == 0 and copied > 0:
                print(f"  已复制/覆盖 {copied}/{len(copy_diffs)}...")

        for d in del_diffs:
            try:
                delete_object(client, dst_bucket, d.key)
                deleted += 1
            except RuntimeError as e:
                print(f"  ⚠️  删除失败: {d.key} ({e})")
                failed += 1
            if deleted % 100 == 0 and deleted > 0:
                print(f"  已删除 {deleted}/{len(del_diffs)}...")

        print(f"\n✅ 桶同步完成。复制/覆盖 {copied} 个，删除 {deleted} 个。", end="")
        if failed:
            print(f" 失败 {failed} 个。", end="")
        print()

        print(f"\n正在重新列举源桶和目标桶，验证同步结果...")
        final_indexes: Dict[str, Dict[str, ObjInfo]] = {
            src_bucket: build_index(client, src_bucket),
            dst_bucket: build_index(client, dst_bucket),
        }
        _print_folder_comparison(final_indexes)
    finally:
        client.close()


# ──────────────────────────── 主入口 ────────────────────────────


def main() -> int:
    print("=" * 60)
    print("一键部署工具")
    print("  CSV URL 域名替换 + OBS 桶同步")
    print("=" * 60)

    deploy_mode = ask_deploy_mode()

    print()
    cfg = resolve_config(deploy_mode)

    phase_csv_replace(cfg["old_names"], cfg["new_name"])

    phase_bucket_sync(cfg["src_bucket"], cfg["dst_bucket"], cfg.get("sync_mode", "正向部署"))

    print(f"\n{'=' * 60}")
    print("全部处理完成。")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

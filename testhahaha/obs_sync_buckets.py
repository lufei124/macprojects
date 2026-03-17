#!/usr/bin/env python3
"""
三桶完全一致同步工具：
  s-project-neo  /  s-project-neo-test  /  s-project-neo-uat

比对规则（基于 MD5 / ETag，完全一致模式）：
  - 源桶有，目标桶没有  → 复制到目标桶
  - 两边都有，MD5 不同  → 用源桶覆盖目标桶
  - 两边都有，MD5 相同  → 跳过
  - 目标桶有，源桶没有  → 从目标桶删除

排除路径：client-db/（该前缀下的对象不参与比对和同步）

执行流程：
  1. 用户输入源桶名（三个之一）
  2. 自动将另外两个桶作为目标桶
  3. 列举三桶、逐个目标桶比对，输出差异 + 日志
  4. 用户确认后执行复制/覆盖/删除，使两个目标桶与源桶完全一致

区域：华东-上海一（cn-east-3）
密钥：直接写在代码 AK / SK 常量中
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, TextIO, Tuple

from obs import ObsClient


SERVER = "https://obs.cn-east-3.myhuaweicloud.com"
AK = "HPUASZIFOROMZZ7HOBCZ"
SK = "c0hxE5Ng43veOFC1xNeRxwhIEldPkCKCI8AXt88a"

ALLOWED_BUCKETS = {
    "s-project-neo",
    "s-project-neo-test",
    "s-project-neo-uat",
}

REASON_MISSING = "目标桶不存在该对象"
REASON_MD5_DIFF = "MD5不同"
REASON_EXTRA = "目标桶多余（源桶不存在）"

SKIP_PREFIXES: List[str] = []


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
    if not AK or AK == "你的AK" or not SK or SK == "你的SK":
        print("❌ 请先在代码顶部填写 AK / SK", file=sys.stderr)
        raise SystemExit(1)
    return ObsClient(access_key_id=AK, secret_access_key=SK, server=SERVER)


def iter_objects(client: ObsClient, bucket: str, prefix: Optional[str] = None) -> Iterable[ObjInfo]:
    """完整分页列举桶内所有对象。"""
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


def build_index(client: ObsClient, bucket: str, prefix: Optional[str]) -> Dict[str, ObjInfo]:
    idx: Dict[str, ObjInfo] = {}
    count = 0
    skip_count = 0
    for obj in iter_objects(client, bucket, prefix=prefix):
        if should_skip(obj.key):
            skip_count += 1
            continue
        idx[obj.key] = obj
        count += 1
        if count % 5000 == 0:
            print(f"  [{bucket}] 已列举 {count} 个对象...")
    if skip_count:
        print(f"  [{bucket}] 列举完成，共 {count} 个对象（跳过 {skip_count} 个）")
    else:
        print(f"  [{bucket}] 列举完成，共 {count} 个对象")
    return idx


def compare_buckets(
    src_index: Dict[str, ObjInfo],
    dst_index: Dict[str, ObjInfo],
) -> Tuple[List[DiffItem], int]:
    """
    比对规则（完全一致模式）：
      1. 源桶有，目标桶没有  → 复制
      2. 两边都有，MD5 不同  → 用源桶覆盖
      3. 两边都有，MD5 相同  → 跳过
      4. 目标桶有，源桶没有  → 从目标桶删除

    返回 (需要同步的 DiffItem 列表, 跳过的数量)
    """
    diffs: List[DiffItem] = []
    skipped = 0

    for key, src_obj in src_index.items():
        dst_obj = dst_index.get(key)

        if dst_obj is None:
            diffs.append(DiffItem(key=key, reason=REASON_MISSING, src_info=src_obj, dst_info=None))
            continue

        if src_obj.etag != dst_obj.etag:
            diffs.append(DiffItem(key=key, reason=REASON_MD5_DIFF, src_info=src_obj, dst_info=dst_obj))
            continue

        skipped += 1

    for key, dst_obj in dst_index.items():
        if key not in src_index:
            diffs.append(DiffItem(key=key, reason=REASON_EXTRA, src_info=dst_obj, dst_info=dst_obj))

    diffs.sort(key=lambda d: d.key)
    return diffs, skipped


def _top_folder(key: str) -> str:
    """取对象 key 的一级文件夹名；没有 / 则归入 '(根目录)'。"""
    pos = key.find("/")
    return key[:pos] if pos > 0 else "(根目录)"


def folder_stats(index: Dict[str, ObjInfo]) -> Dict[str, Tuple[int, int]]:
    """返回 {一级文件夹: (文件数, 总大小字节)}，按文件夹名排序。"""
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


def write_folder_comparison(
    log: TextIO,
    all_indexes: Dict[str, Dict[str, ObjInfo]],
) -> None:
    """在日志中写入三个桶按一级文件夹的文件数 / 大小对比表。"""
    bucket_names = sorted(all_indexes.keys())
    stats_map = {b: folder_stats(idx) for b, idx in all_indexes.items()}

    all_folders = sorted({f for s in stats_map.values() for f in s})

    log.write("\n" + "=" * 90 + "\n")
    log.write("三桶一级文件夹 文件数 / 大小 对比\n")
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
    """在终端打印三桶按一级文件夹的文件数 / 大小对比表。"""
    bucket_names = sorted(all_indexes.keys())
    stats_map = {b: folder_stats(idx) for b, idx in all_indexes.items()}
    all_folders = sorted({f for s in stats_map.values() for f in s})

    print(f"\n{'=' * 90}")
    print("同步后三桶一级文件夹 文件数 / 大小 对比")
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


def ask_bucket(prompt: str) -> str:
    """交互式让用户输入桶名，必须是允许的三个之一。"""
    allowed = sorted(ALLOWED_BUCKETS)
    while True:
        print(f"\n{prompt}")
        print(f"可选桶: {', '.join(allowed)}")
        name = input("请输入桶名: ").strip()
        if name not in ALLOWED_BUCKETS:
            print(f"❌ 错误：'{name}' 不在允许范围内，只能填以下三个之一：")
            print(f"   {', '.join(allowed)}")
            continue
        return name


def write_diff_log(
    log: TextIO,
    src_bucket: str,
    dst_bucket: str,
    src_index: Dict[str, ObjInfo],
    dst_index: Dict[str, ObjInfo],
    diffs: List[DiffItem],
    skipped: int,
    all_indexes: Optional[Dict[str, Dict[str, ObjInfo]]] = None,
) -> None:
    log.write("=" * 70 + "\n")
    log.write("源桶 → 目标桶 差异同步日志\n")
    log.write("=" * 70 + "\n")
    log.write(f"同步时间       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log.write(f"源桶           : {src_bucket}\n")
    log.write(f"目标桶         : {dst_bucket}\n")
    log.write(f"源桶对象总数   : {len(src_index)}\n")
    log.write(f"目标桶对象总数 : {len(dst_index)}\n")
    log.write(f"需要同步       : {len(diffs)} 个对象\n")
    log.write(f"无需同步(跳过) : {skipped} 个对象\n")
    log.write("=" * 70 + "\n")
    log.write("\n比对规则（完全一致模式，基于 MD5 / ETag）：\n")
    log.write("  1. 源桶有，目标桶没有  → 复制到目标桶\n")
    log.write("  2. 两边都有，MD5 不同  → 用源桶覆盖\n")
    log.write("  3. 两边都有，MD5 相同  → 跳过\n")
    log.write("  4. 目标桶有，源桶没有  → 从目标桶删除\n")
    log.write("\n")

    if all_indexes:
        write_folder_comparison(log, all_indexes)

    # 按差异原因分组统计
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


def _print_and_sync_one_target(
    client: ObsClient,
    src_bucket: str,
    src_index: Dict[str, ObjInfo],
    dst_bucket: str,
    dst_index: Dict[str, ObjInfo],
    all_indexes: Dict[str, Dict[str, ObjInfo]],
    log: TextIO,
) -> None:
    """对单个目标桶进行比对、展示、确认、同步，日志写入传入的 log 句柄。"""
    diffs, skipped = compare_buckets(src_index, dst_index)

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
    print(f"    - 需删除(目标桶多余): {extra_count} 个")
    print(f"  无需同步(跳过):         {skipped} 个")
    print(f"{'=' * 60}")

    write_diff_log(log, src_bucket, dst_bucket, src_index, dst_index, diffs, skipped, all_indexes)

    if not diffs:
        print(f"✅ {dst_bucket} 已与源桶完全一致，无需同步。")
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

    confirm = input(
        f"\n是否执行同步？（复制/覆盖 {len(copy_diffs)} 个，删除 {len(del_diffs)} 个）(y/n): "
    ).strip().lower()
    if confirm != "y":
        print("已跳过该目标桶。")
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

    print(f"\n✅ {dst_bucket} 同步完成。复制/覆盖 {copied} 个，删除 {deleted} 个。", end="")
    if failed:
        print(f" 失败 {failed} 个。", end="")
    print()


def main() -> int:
    src_bucket = "s-project-neo"
    dst_buckets = ["s-project-neo-test", "s-project-neo-uat"]

    print(f"\n源桶: {src_bucket}")
    print(f"目标桶: {', '.join(dst_buckets)}（将与源桶完全一致）")

    client = get_client()
    try:
        print(f"\n正在列举三桶对象...")
        src_index = build_index(client, src_bucket, prefix=None)
        all_indexes: Dict[str, Dict[str, ObjInfo]] = {src_bucket: src_index}
        for db in dst_buckets:
            all_indexes[db] = build_index(client, db, prefix=None)

        log_name = f"sync_{src_bucket}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_name, "w", encoding="utf-8") as log:
            for db in dst_buckets:
                _print_and_sync_one_target(
                    client, src_bucket, src_index, db, all_indexes[db], all_indexes, log,
                )
                log.write("\n\n")

        print(f"\n差异日志已写入: {os.path.abspath(log_name)}")

        # 同步后重新列举三桶，输出最终对比
        print(f"\n正在重新列举三桶，验证同步结果...")
        final_indexes: Dict[str, Dict[str, ObjInfo]] = {}
        for b in [src_bucket] + dst_buckets:
            final_indexes[b] = build_index(client, b, prefix=None)

        _print_folder_comparison(final_indexes)

        print(f"\n{'=' * 60}")
        print("全部处理完成。")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())

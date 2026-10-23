#!/usr/bin/env bash
# 本地全量回归（与 .github/workflows/ci.yml 的 backend-regression 等价）
# ★ 必须用 Python 3.10：PATH 上的 python 是 3.13，没装依赖。
cd "$(dirname "$0")" || exit 1
PY="C:/Users/DELL-PC/AppData/Local/Programs/Python/Python310/python.exe"
unset DATABASE_URL
fail=0
total=0
failed_list=""
for t in tests/test_*.py; do
  total=$((total + 1))
  out=$("$PY" "$t" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    fail=$((fail + 1))
    failed_list="$failed_list\n  - $t (rc=$rc)"
    echo "===== [FAIL] $t ====="
    echo "$out" | tail -25
  fi
done
echo ""
echo "----------------------------------------"
echo "[TOTAL] $total 套件，[FAIL] $fail"
if [ "$fail" -ne 0 ]; then
  echo -e "失败清单:$failed_list"
  exit 1
fi
echo "全部通过"

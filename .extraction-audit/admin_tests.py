from pathlib import Path
p=Path('backend/tests/test_admin_role.py');s=p.read_text(encoding='utf-8')
pos=s.index('        print("\\n[14]')
s=s[:pos]+'''        print("\\n[13b] 三个管理员端点统一鉴权与踢下线 404 契约")
        for method, path, payload in [
            ("GET", "/api/admin/user-logs", None),
            ("POST", "/api/admin/add-job", {"job_name": "guard", "city": "北京"}),
            ("POST", "/api/admin/users/999999/kick", None),
        ]:
            for label, token, expected in [("缺失", None, 401), ("伪造", "invalid", 401), ("普通用户", user_token, 403)]:
                headers = {} if token is None else {"Authorization": f"Bearer {token}"}
                response = await c.request(method, path, headers=headers, json=payload)
                _check(f"{path} {label}凭证 -> {expected}", response.status_code == expected)
        missing = await c.post("/api/admin/users/999999/kick", headers={"Authorization": f"Bearer {admin_token}"})
        _check("管理员踢不存在的用户 -> 404 原文案", missing.status_code == 404 and missing.json() == {"detail": "用户不存在"})

'''+s[pos:];p.write_text(s,encoding='utf-8')

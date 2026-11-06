import os
import hmac
import json
import html
import hashlib
import logging
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

logger = logging.getLogger("PromoWebApp")
IST = timezone(timedelta(hours=5, minutes=30))

webapp_router = APIRouter()

# Global references injected from promo_server.py
pg_pool = None
bot_instance = None
ADMIN_OWNER_ID = 8431723612
BOT_TOKEN_REF = ""

HTML_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rewards.html")


def init_webapp_dependencies(pool, bot, admin_id: int = 8431723612, bot_token: str = ""):
    global pg_pool, bot_instance, ADMIN_OWNER_ID, BOT_TOKEN_REF
    pg_pool = pool
    bot_instance = bot
    ADMIN_OWNER_ID = admin_id
    BOT_TOKEN_REF = bot_token


def verify_telegram_webapp_init_data(init_data: str) -> Optional[Dict[str, Any]]:
    """
    Cryptographically verifies Telegram WebApp initData using HMAC-SHA256
    and returns the parsed user dictionary if valid.
    """
    if not init_data or not BOT_TOKEN_REF:
        return None
    try:
        parsed_data = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed_data.pop("hash", None)
        if not received_hash:
            return None

        data_check_arr = [f"{k}={v}" for k, v in sorted(parsed_data.items())]
        data_check_string = "\n".join(data_check_arr)

        secret_key = hmac.new(
            b"WebAppData",
            BOT_TOKEN_REF.encode("utf-8"),
            hashlib.sha256
        ).digest()

        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(calculated_hash, received_hash):
            return None

        user_json = parsed_data.get("user")
        if not user_json:
            return None

        user_data = json.loads(user_json)
        if not user_data or "id" not in user_data:
            return None

        return user_data
    except Exception as e:
        logger.error(f"initData verification error: {e}")
        return None


async def compute_and_sync_owner_wallet(conn, user_id: int) -> Tuple[float, float, float, Optional[str]]:
    """
    Calculates (total_balance, withdrawable_balance, locked_balance, cached_owner_name)
    accounting for:
    - 3-day maturation on ad earnings
    - 7-day maturation on GC addition bonuses (only if not clawed back/revoked)
    - Withdrawals (PENDING or SUCCESS)
    - Clawback penalties
    - Balance spent on purchasing Promotional Ads (owner_ad_spends)
    """
    now_utc = datetime.now(timezone.utc)

    matured_sum = await conn.fetchval('''
        SELECT COALESCE(SUM(amount), 0)
        FROM owner_earning_logs
        WHERE owner_id = $1
          AND is_revoked = FALSE
          AND available_at <= $2;
    ''', user_id, now_utc)

    locked_sum = await conn.fetchval('''
        SELECT COALESCE(SUM(amount), 0)
        FROM owner_earning_logs
        WHERE owner_id = $1
          AND is_revoked = FALSE
          AND available_at > $2;
    ''', user_id, now_utc)

    withdrawn_sum = await conn.fetchval('''
        SELECT COALESCE(SUM(amount), 0)
        FROM owner_withdrawals
        WHERE owner_id = $1
          AND status IN ('PENDING', 'SUCCESS');
    ''', user_id)

    clawback_penalty = await conn.fetchval('''
        SELECT COALESCE(SUM(claimed_amount), 0)
        FROM gc_reward_claims
        WHERE owner_id = $1
          AND clawed_back = TRUE
          AND clawback_deducted_from_matured = TRUE;
    ''', user_id)

    ad_spent_sum = await conn.fetchval('''
        SELECT COALESCE(SUM(amount), 0)
        FROM owner_ad_spends
        WHERE owner_id = $1;
    ''', user_id)

    raw_matured_net = float(matured_sum) - float(withdrawn_sum) - float(clawback_penalty)
    ad_spent_val = float(ad_spent_sum)

    # Deduct ad spends from matured balance first, then from locked balance if needed
    if ad_spent_val <= max(0.0, raw_matured_net):
        withdrawable_bal = max(0.0, raw_matured_net - ad_spent_val)
        rem_ad_spend = 0.0
    else:
        rem_ad_spend = ad_spent_val - max(0.0, raw_matured_net)
        withdrawable_bal = 0.0

    effective_locked = max(0.0, float(locked_sum) - rem_ad_spend + min(0.0, raw_matured_net))
    total_bal = max(0.0, withdrawable_bal + effective_locked)

    wallet_row = await conn.fetchrow('''
        INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (owner_id) DO UPDATE SET
            total_balance = $2,
            withdrawable_balance = $3,
            updated_at = NOW()
        RETURNING owner_name;
    ''', user_id, round(total_bal, 4), round(withdrawable_bal, 4))

    cached_name = wallet_row["owner_name"] if wallet_row else None
    return total_bal, withdrawable_bal, effective_locked, cached_name


@webapp_router.get("/api/owner_data")
async def get_owner_dashboard_data(request: Request):
    """Verifies Telegram WebApp initData and returns owner profile, balances, and 7d/30d earnings graphs."""
    if not pg_pool:
        raise HTTPException(status_code=503, detail="Database connecting")

    init_data = request.headers.get("X-Telegram-Init-Data", "")
    tg_user = verify_telegram_webapp_init_data(init_data)
    if not tg_user:
        return JSONResponse(
            {"status": "error", "message": "Unauthorized: Invalid or missing Telegram WebApp verification."},
            status_code=401
        )

    user_id = int(tg_user["id"])
    now_utc = datetime.now(timezone.utc)
    now_ist = datetime.now(IST)
    today_ist = now_ist.date()

    async with pg_pool.acquire() as conn:
        total_bal, withdrawable_bal, locked_bal, cached_name = await compute_and_sync_owner_wallet(conn, user_id)

        user_row = await conn.fetchrow('''
            SELECT first_name, username FROM users WHERE user_id = $1;
        ''', user_id)

        owner_name = (
            tg_user.get("first_name")
            or cached_name
            or (user_row["first_name"] if user_row and user_row["first_name"] else None)
            or f"Partner #{user_id}"
        )

        start_of_day_utc = datetime(today_ist.year, today_ist.month, today_ist.day, tzinfo=IST).astimezone(timezone.utc)
        withdrawn_today = await conn.fetchval('''
            SELECT COALESCE(SUM(amount), 0)
            FROM owner_withdrawals
            WHERE owner_id = $1
              AND status IN ('PENDING', 'SUCCESS')
              AND created_at >= $2;
        ''', user_id, start_of_day_utc)

        cutoff_30d = now_utc - timedelta(days=30)
        log_rows = await conn.fetch('''
            SELECT (created_at AT TIME ZONE 'Asia/Kolkata')::date AS earn_date,
                   COALESCE(SUM(amount), 0) AS day_total
            FROM owner_earning_logs
            WHERE owner_id = $1
              AND is_revoked = FALSE
              AND created_at >= $2
            GROUP BY earn_date
            ORDER BY earn_date ASC;
        ''', user_id, cutoff_30d)

        recent_withdrawals = await conn.fetch('''
            SELECT id, amount, upi_id, status, created_at
            FROM owner_withdrawals
            WHERE owner_id = $1
            ORDER BY created_at DESC
            LIMIT 5;
        ''', user_id)

    day_map = {str(r["earn_date"]): round(float(r["day_total"]), 2) for r in log_rows}

    labels_30d = []
    values_30d = []
    for i in range(29, -1, -1):
        d = today_ist - timedelta(days=i)
        d_str = str(d)
        labels_30d.append(d.strftime("%d %b"))
        values_30d.append(day_map.get(d_str, 0.0))

    labels_7d = labels_30d[-7:]
    values_7d = values_30d[-7:]

    avatar_url = f"https://ui-avatars.com/api/?name={owner_name}&background=FFE8CC&color=8A4B08&bold=true"
    if bot_instance:
        try:
            photos = await bot_instance.get_user_profile_photos(user_id, limit=1)
            if photos and photos.total_count > 0:
                file_id = photos.photos[0][-1].file_id
                tg_file = await bot_instance.get_file(file_id)
                if tg_file and tg_file.file_path:
                    avatar_url = tg_file.file_path
        except Exception:
            pass

    history_list = [
        {
            "id": r["id"],
            "amount": f"{float(r['amount']):.2f}",
            "upi_id": r["upi_id"],
            "status": r["status"],
            "date": r["created_at"].astimezone(IST).strftime("%d %b, %I:%M %p")
        }
        for r in recent_withdrawals
    ]

    return JSONResponse({
        "status": "ok",
        "owner_id": user_id,
        "owner_name": owner_name,
        "avatar_url": avatar_url,
        "total_balance": round(total_bal, 2),
        "withdrawable_balance": round(withdrawable_bal, 2),
        "locked_balance": round(locked_bal, 2),
        "withdrawn_today": round(float(withdrawn_today or 0), 2),
        "remaining_daily_limit": round(max(0.0, 500.0 - float(withdrawn_today or 0)), 2),
        "chart_7d": {"labels": labels_7d, "values": values_7d, "total": round(sum(values_7d), 2)},
        "chart_30d": {"labels": labels_30d, "values": values_30d, "total": round(sum(values_30d), 2)},
        "withdrawals": history_list
    })


@webapp_router.post("/api/withdraw")
async def process_owner_withdrawal(request: Request):
    """Validates Telegram initData and creates a withdrawal request (Min ₹100, Max ₹500/day)."""
    if not pg_pool:
        raise HTTPException(status_code=503, detail="Database offline")

    body = await request.json()
    init_data = request.headers.get("X-Telegram-Init-Data") or str(body.get("init_data", ""))
    tg_user = verify_telegram_webapp_init_data(init_data)
    if not tg_user:
        return JSONResponse(
            {"status": "error", "message": "Unauthorized: Invalid Telegram WebApp session."},
            status_code=401
        )

    owner_id = int(tg_user["id"])
    try:
        amount = round(float(body.get("amount", 0)), 2)
    except Exception:
        return JSONResponse({"status": "error", "message": "Invalid withdrawal amount."}, status_code=400)

    upi_id = str(body.get("upi_id", "")).strip()
    mobile_no = str(body.get("mobile_no", "")).strip() or "Not Provided"

    if amount < 100.0 or amount > 500.0:
        return JSONResponse({"status": "error", "message": "Withdrawal amount must be between ₹100 and ₹500."}, status_code=400)
    if "@" not in upi_id or len(upi_id) < 5:
        return JSONResponse({"status": "error", "message": "Please enter a valid UPI ID (e.g. name@okaxis)."}, status_code=400)

    today_ist = datetime.now(IST).date()
    start_of_day_utc = datetime(today_ist.year, today_ist.month, today_ist.day, tzinfo=IST).astimezone(timezone.utc)

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('''
                INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance)
                VALUES ($1, 0, 0)
                ON CONFLICT (owner_id) DO NOTHING;
            ''', owner_id)
            await conn.fetchrow("SELECT owner_id FROM owner_wallets WHERE owner_id = $1 FOR UPDATE;", owner_id)

            total_bal, withdrawable_bal, locked_bal, cached_name = await compute_and_sync_owner_wallet(conn, owner_id)

            if withdrawable_bal < 100.0:
                return JSONResponse({
                    "status": "error",
                    "message": f"Minimum withdrawable balance required is ₹100.00 (Available: ₹{withdrawable_bal:.2f}). Ad earnings unlock in 3 days & GC bonuses unlock after 7 days."
                }, status_code=400)

            if amount > withdrawable_bal:
                return JSONResponse({
                    "status": "error",
                    "message": f"Insufficient withdrawable balance! You can withdraw up to ₹{withdrawable_bal:.2f}."
                }, status_code=400)

            withdrawn_today = await conn.fetchval('''
                SELECT COALESCE(SUM(amount), 0)
                FROM owner_withdrawals
                WHERE owner_id = $1 AND status IN ('PENDING', 'SUCCESS') AND created_at >= $2;
            ''', owner_id, start_of_day_utc)

            if float(withdrawn_today or 0) + amount > 500.0:
                rem = max(0.0, 500.0 - float(withdrawn_today or 0))
                return JSONResponse({
                    "status": "error",
                    "message": f"Daily withdrawal limit is ₹500.00! You can withdraw ₹{rem:.2f} more today."
                }, status_code=400)

            user_row = await conn.fetchrow("SELECT first_name, username FROM users WHERE user_id = $1;", owner_id)
            owner_name = (
                tg_user.get("first_name")
                or (user_row["first_name"] if user_row and user_row["first_name"] else None)
                or cached_name
                or f"Owner #{owner_id}"
            )
            username_str = f"@{user_row['username']}" if (user_row and user_row["username"]) else (
                f"@{tg_user['username']}" if tg_user.get("username") else "N/A"
            )

            wd_id = await conn.fetchval('''
                INSERT INTO owner_withdrawals (owner_id, owner_name, amount, upi_id, mobile_no, status, created_at)
                VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW())
                RETURNING id;
            ''', owner_id, owner_name, amount, upi_id, mobile_no)

            new_withdrawable = max(0.0, withdrawable_bal - amount)
            new_total = max(0.0, new_withdrawable + locked_bal)

            await conn.execute('''
                UPDATE owner_wallets
                SET total_balance = $1, withdrawable_balance = $2, updated_at = NOW()
                WHERE owner_id = $3;
            ''', round(new_total, 4), round(new_withdrawable, 4), owner_id)

    if bot_instance:
        safe_name = html.escape(owner_name)
        safe_upi = html.escape(upi_id)
        safe_mob = html.escape(mobile_no)
        owner_mention = f'<a href="tg://user?id={owner_id}">{safe_name}</a>'

        admin_card = (
            f"🏦 <b>NEW GC OWNER WITHDRAWAL REQUEST</b>\n\n"
            f"<blockquote>🆔 <b>Request ID:</b> <code>#WD{wd_id}</code>\n"
            f"👤 <b>GC Owner:</b> {owner_mention} ({username_str})\n"
            f"🔢 <b>Owner UID:</b> <code>{owner_id}</code>\n"
            f"📱 <b>Mobile No:</b> <code>{safe_mob}</code></blockquote>\n"
            f"<blockquote>💸 <b>Withdrawal Amount:</b> <code>₹{amount:.2f}</code>\n"
            f"🏧 <b>UPI ID:</b> <code>{safe_upi}</code>\n"
            f"💰 <b>Remaining Withdrawable:</b> <code>₹{new_withdrawable:.2f}</code>\n"
            f"🏛️ <b>Total Remaining Balance:</b> <code>₹{new_total:.2f}</code></blockquote>\n"
            f"👇 <i>Transfer the amount to the UPI ID above and click Success or Deny below:</i>"
        )
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Success (Approve)", callback_data=f"wd_approve_{wd_id}"),
                InlineKeyboardButton("❌ Deny (Refund)", callback_data=f"wd_deny_{wd_id}")
            ]
        ])
        try:
            await bot_instance.send_message(
                chat_id=ADMIN_OWNER_ID,
                text=admin_card,
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
        except Exception as e:
            logger.error(f"Failed to send withdrawal alert to admin: {e}")

        owner_receipt = (
            f"📤 <b>WITHDRAWAL REQUEST SUBMITTED!</b> ✨\n\n"
            f"<blockquote>🆔 <b>Reference ID:</b> <code>#WD{wd_id}</code>\n"
            f"💸 <b>Requested Amount:</b> <code>₹{amount:.2f}</code>\n"
            f"🏧 <b>Destination UPI:</b> <code>{safe_upi}</code>\n"
            f"⏳ <b>Current Status:</b> <b>Pending Admin Verification</b></blockquote>\n"
            f"<i>Your payout is being processed and will be credited to your UPI account shortly!</i>"
        )
        try:
            await bot_instance.send_message(chat_id=owner_id, text=owner_receipt, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    return JSONResponse({
        "status": "ok",
        "message": f"Withdrawal request #WD{wd_id} for ₹{amount:.2f} submitted successfully!",
        "new_withdrawable": round(new_withdrawable, 2),
        "new_total": round(new_total, 2)
    })


@webapp_router.get("/rewards", response_class=HTMLResponse)
async def serve_rewards_webapp():
    """Serves the external rewards.html file."""
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    return HTMLResponse("<h3>rewards.html file not found on server.</h3>", status_code=404)

import os
import time
import html
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

logger = logging.getLogger("PromoWebApp")
IST = timezone(timedelta(hours=5, minutes=30))

webapp_router = APIRouter()

# Global references injected from promo_server.py
pg_pool = None
bot_instance = None
ADMIN_OWNER_ID = 8431723612


def init_webapp_dependencies(pool, bot, admin_id: int = 8431723612):
    global pg_pool, bot_instance, ADMIN_OWNER_ID
    pg_pool = pool
    bot_instance = bot
    ADMIN_OWNER_ID = admin_id


@webapp_router.get("/api/owner_data/{user_id}")
async def get_owner_dashboard_data(user_id: int):
    """Fetches owner profile, total vs withdrawable (3-day matured) balance, and 7d/30d graph series."""
    if not pg_pool:
        raise HTTPException(status_code=503, detail="Database connecting")

    now_utc = datetime.now(timezone.utc)
    now_ist = datetime.now(IST)
    today_ist = now_ist.date()

    async with pg_pool.acquire() as conn:
        # 1. Recalculate matured balance (credits older than 3 days that are not revoked)
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

        withdrawable_bal = max(0.0, float(matured_sum) - float(withdrawn_sum) - float(clawback_penalty))
        total_bal = max(0.0, withdrawable_bal + float(locked_sum))

        # Sync wallet snapshot
        wallet_row = await conn.fetchrow('''
            INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (owner_id) DO UPDATE SET
                total_balance = $2,
                withdrawable_balance = $3,
                updated_at = NOW()
            RETURNING owner_name, photo_url;
        ''', user_id, round(total_bal, 4), round(withdrawable_bal, 4))

        user_row = await conn.fetchrow('''
            SELECT first_name, username FROM users WHERE user_id = $1;
        ''', user_id)

        owner_name = (
            (wallet_row["owner_name"] if wallet_row and wallet_row["owner_name"] else None)
            or (user_row["first_name"] if user_row and user_row["first_name"] else None)
            or f"Partner #{user_id}"
        )

        # Check today's already withdrawn amount (for ₹500/day cap)
        start_of_day_utc = datetime(today_ist.year, today_ist.month, today_ist.day, tzinfo=IST).astimezone(timezone.utc)
        withdrawn_today = await conn.fetchval('''
            SELECT COALESCE(SUM(amount), 0)
            FROM owner_withdrawals
            WHERE owner_id = $1
              AND status IN ('PENDING', 'SUCCESS')
              AND created_at >= $2;
        ''', user_id, start_of_day_utc)

        # 2. Fetch last 30 days daily earnings for 7d & 30d graphs
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

    # Fetch profile photo via Bot API if not cached
    avatar_url = f"https://ui-avatars.com/api/?name={owner_name}&background=FFE8CC&color=8A4B08&bold=true"
    if bot_instance:
        try:
            chat_obj = await bot_instance.get_chat(user_id)
            if chat_obj and chat_obj.first_name:
                owner_name = chat_obj.first_name
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
        "locked_balance": round(max(0.0, total_bal - withdrawable_bal), 2),
        "withdrawn_today": round(float(withdrawn_today or 0), 2),
        "remaining_daily_limit": round(max(0.0, 500.0 - float(withdrawn_today or 0)), 2),
        "chart_7d": {"labels": labels_7d, "values": values_7d, "total": round(sum(values_7d), 2)},
        "chart_30d": {"labels": labels_30d, "values": values_30d, "total": round(sum(values_30d), 2)},
        "withdrawals": history_list
    })


@webapp_router.post("/api/withdraw")
async def process_owner_withdrawal(request: Request):
    """Validates and creates a withdrawal request (Min ₹100, Max ₹500/day) and alerts Bot Owner."""
    if not pg_pool:
        raise HTTPException(status_code=503, detail="Database offline")

    body = await request.json()
    try:
        owner_id = int(body.get("owner_id", 0))
        amount = round(float(body.get("amount", 0)), 2)
    except Exception:
        return JSONResponse({"status": "error", "message": "Invalid amount or user ID."}, status_code=400)

    upi_id = str(body.get("upi_id", "")).strip()
    mobile_no = str(body.get("mobile_no", "")).strip() or "Not Provided"

    if owner_id <= 0:
        return JSONResponse({"status": "error", "message": "Unable to identify your Telegram account."}, status_code=400)
    if amount < 100.0 or amount > 500.0:
        return JSONResponse({"status": "error", "message": "Withdrawal amount must be between ₹100 and ₹500."}, status_code=400)
    if "@" not in upi_id or len(upi_id) < 5:
        return JSONResponse({"status": "error", "message": "Please enter a valid UPI ID (e.g. name@okaxis)."}, status_code=400)

    now_utc = datetime.now(timezone.utc)
    today_ist = datetime.now(IST).date()
    start_of_day_utc = datetime(today_ist.year, today_ist.month, today_ist.day, tzinfo=IST).astimezone(timezone.utc)

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            # Lock owner wallet row
            await conn.execute('''
                INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance)
                VALUES ($1, 0, 0)
                ON CONFLICT (owner_id) DO NOTHING;
            ''', owner_id)
            wallet_row = await conn.fetchrow('''
                SELECT owner_name FROM owner_wallets WHERE owner_id = $1 FOR UPDATE;
            ''', owner_id)

            matured_sum = await conn.fetchval('''
                SELECT COALESCE(SUM(amount), 0)
                FROM owner_earning_logs
                WHERE owner_id = $1 AND is_revoked = FALSE AND available_at <= $2;
            ''', owner_id, now_utc)

            locked_sum = await conn.fetchval('''
                SELECT COALESCE(SUM(amount), 0)
                FROM owner_earning_logs
                WHERE owner_id = $1 AND is_revoked = FALSE AND available_at > $2;
            ''', owner_id, now_utc)

            withdrawn_sum = await conn.fetchval('''
                SELECT COALESCE(SUM(amount), 0)
                FROM owner_withdrawals
                WHERE owner_id = $1 AND status IN ('PENDING', 'SUCCESS');
            ''', owner_id)

            clawback_penalty = await conn.fetchval('''
                SELECT COALESCE(SUM(claimed_amount), 0)
                FROM gc_reward_claims
                WHERE owner_id = $1 AND clawed_back = TRUE AND clawback_deducted_from_matured = TRUE;
            ''', owner_id)

            withdrawable_bal = max(0.0, float(matured_sum) - float(withdrawn_sum) - float(clawback_penalty))

            if withdrawable_bal < 100.0:
                return JSONResponse({
                    "status": "error",
                    "message": f"Minimum withdrawable balance required is ₹100.00 (Available: ₹{withdrawable_bal:.2f}). Note: New earnings unlock 3 days after credit."
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
                (user_row["first_name"] if user_row and user_row["first_name"] else None)
                or (wallet_row["owner_name"] if wallet_row and wallet_row["owner_name"] else None)
                or f"Owner #{owner_id}"
            )
            username_str = f"@{user_row['username']}" if (user_row and user_row["username"]) else "N/A"

            wd_id = await conn.fetchval('''
                INSERT INTO owner_withdrawals (owner_id, owner_name, amount, upi_id, mobile_no, status, created_at)
                VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW())
                RETURNING id;
            ''', owner_id, owner_name, amount, upi_id, mobile_no)

            new_withdrawable = max(0.0, withdrawable_bal - amount)
            new_total = max(0.0, new_withdrawable + float(locked_sum))

            await conn.execute('''
                UPDATE owner_wallets
                SET total_balance = $1, withdrawable_balance = $2, updated_at = NOW()
                WHERE owner_id = $3;
            ''', round(new_total, 4), round(new_withdrawable, 4), owner_id)

    # Notify Bot Owner (Admin) with Approve / Deny inline buttons
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

        # Also send receipt to Owner DM
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
async def serve_rewards_webapp(request: Request, uid: Optional[int] = 0):
    """Serves the modern Light Yellowish-Pink & Butter-Yellow GC Owner Earnings WebApp."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
    <title>Partner Rewards & Creator Earnings</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-pink-cream: #FFF4F6;
            --bg-butter-yellow: #FFFDF0;
            --card-butter: #FFFBEB;
            --card-blush: #FFF0F3;
            --border-soft: #FDE68A;
            --border-pink: #FECDD3;
            --text-dark: #451A03;
            --text-muted: #92400E;
            --accent-gold: #D97706;
            --accent-rose: #E11D48;
            --btn-gradient: linear-gradient(135deg, #F59E0B 0%, #EC4899 100%);
        }
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Plus Jakarta Sans', sans-serif;
            -webkit-tap-highlight-color: transparent;
        }
        body {
            background: linear-gradient(160deg, var(--bg-pink-cream) 0%, var(--bg-butter-yellow) 55%, #FFF5F7 100%);
            color: var(--text-dark);
            min-height: 100vh;
            padding: 16px 14px 36px 14px;
        }
        .container {
            max-width: 480px;
            margin: 0 auto;
        }
        /* Top Profile Card */
        .profile-card {
            background: linear-gradient(135deg, #FFFDF4 0%, #FFF0F5 100%);
            border: 1.5px solid var(--border-soft);
            border-radius: 22px;
            padding: 18px;
            display: flex;
            align-items: center;
            gap: 14px;
            box-shadow: 0 10px 25px rgba(245, 158, 11, 0.08);
            margin-bottom: 16px;
        }
        .avatar {
            width: 64px;
            height: 64px;
            border-radius: 50%;
            object-fit: cover;
            border: 3px solid #FDE047;
            box-shadow: 0 4px 12px rgba(225, 29, 72, 0.15);
            background: #FFF9DB;
        }
        .profile-info h2 {
            font-size: 1.15rem;
            font-weight: 800;
            color: var(--text-dark);
        }
        .badge-partner {
            display: inline-block;
            margin-top: 4px;
            background: #FEF3C7;
            color: #B45309;
            font-size: 0.72rem;
            font-weight: 700;
            padding: 4px 10px;
            border-radius: 999px;
            border: 1px solid #FDE68A;
        }
        /* Balance Grid */
        .balance-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            margin-bottom: 14px;
        }
        .bal-box {
            background: var(--card-butter);
            border: 1.5px solid var(--border-soft);
            border-radius: 20px;
            padding: 16px 14px;
            box-shadow: 0 6px 18px rgba(217, 119, 6, 0.06);
        }
        .bal-box.highlight {
            background: linear-gradient(145deg, #FFFBEB 0%, #FFE4E6 100%);
            border-color: var(--border-pink);
        }
        .bal-label {
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.4px;
            color: var(--text-muted);
        }
        .bal-value {
            font-size: 1.55rem;
            font-weight: 800;
            color: var(--text-dark);
            margin-top: 6px;
        }
        .bal-sub {
            font-size: 0.7rem;
            color: #B45309;
            margin-top: 4px;
            font-weight: 600;
        }
        /* Withdraw CTA */
        .withdraw-card {
            background: linear-gradient(135deg, #FFF9DB 0%, #FFEAF0 100%);
            border: 1.5px solid #FCD34D;
            border-radius: 20px;
            padding: 16px;
            margin-bottom: 18px;
            text-align: center;
        }
        .btn-main {
            width: 100%;
            background: var(--btn-gradient);
            color: #FFFFFF;
            border: none;
            padding: 14px 20px;
            font-size: 0.96rem;
            font-weight: 800;
            border-radius: 14px;
            cursor: pointer;
            box-shadow: 0 8px 20px rgba(236, 72, 153, 0.25);
            transition: transform 0.15s ease, opacity 0.15s ease;
        }
        .btn-main:active {
            transform: scale(0.98);
        }
        .btn-main:disabled {
            opacity: 0.55;
            cursor: not-allowed;
        }
        .withdraw-hint {
            font-size: 0.74rem;
            color: var(--text-muted);
            margin-top: 8px;
            font-weight: 600;
        }
        /* Analytics Graph Card */
        .chart-card {
            background: #FFFDF7;
            border: 1.5px solid var(--border-soft);
            border-radius: 22px;
            padding: 16px;
            margin-bottom: 16px;
            box-shadow: 0 8px 22px rgba(217, 119, 6, 0.06);
        }
        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        .chart-title {
            font-size: 0.95rem;
            font-weight: 800;
            color: var(--text-dark);
        }
        .tab-group {
            display: flex;
            background: #FEF3C7;
            padding: 3px;
            border-radius: 10px;
            gap: 4px;
        }
        .tab-btn {
            border: none;
            background: transparent;
            color: #92400E;
            font-size: 0.74rem;
            font-weight: 700;
            padding: 5px 10px;
            border-radius: 8px;
            cursor: pointer;
        }
        .tab-btn.active {
            background: #FFFFFF;
            color: #D97706;
            box-shadow: 0 2px 6px rgba(0,0,0,0.06);
        }
        /* Recent Withdrawals */
        .history-card {
            background: #FFFDF7;
            border: 1.5px solid var(--border-soft);
            border-radius: 20px;
            padding: 15px;
            margin-bottom: 16px;
        }
        .history-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px dashed #FDE68A;
            font-size: 0.82rem;
        }
        .history-item:last-child {
            border-bottom: none;
        }
        .status-pill {
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 0.7rem;
            font-weight: 700;
        }
        .status-PENDING { background: #FEF3C7; color: #D97706; }
        .status-SUCCESS { background: #D1FAE5; color: #059669; }
        .status-DENIED { background: #FFE4E6; color: #E11D48; }
        /* Revenue Share Footer */
        .rev-banner {
            background: linear-gradient(135deg, #FFF0F3 0%, #FEF9C3 100%);
            border: 1.5px solid var(--border-pink);
            border-radius: 18px;
            padding: 14px 16px;
            text-align: center;
            font-size: 0.8rem;
            line-height: 1.45;
            color: #881337;
            font-weight: 600;
        }
        /* Modal Overlay */
        .modal-backdrop {
            position: fixed;
            inset: 0;
            background: rgba(69, 26, 3, 0.45);
            backdrop-filter: blur(4px);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 16px;
            z-index: 999;
        }
        .modal-box {
            background: linear-gradient(160deg, #FFFDF5 0%, #FFF0F3 100%);
            border: 2px solid #FDE68A;
            border-radius: 24px;
            width: 100%;
            max-width: 400px;
            padding: 22px 18px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.18);
        }
        .input-group {
            margin-bottom: 14px;
            text-align: left;
        }
        .input-group label {
            display: block;
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--text-muted);
            margin-bottom: 5px;
        }
        .input-field {
            width: 100%;
            padding: 12px 14px;
            border-radius: 12px;
            border: 1.5px solid #FCD34D;
            background: #FFFFFF;
            font-size: 0.92rem;
            color: var(--text-dark);
            outline: none;
        }
        .modal-actions {
            display: flex;
            gap: 10px;
            margin-top: 16px;
        }
        .btn-secondary {
            flex: 1;
            padding: 12px;
            border-radius: 12px;
            border: 1.5px solid #FCD34D;
            background: #FFFBEB;
            color: var(--text-muted);
            font-weight: 700;
            cursor: pointer;
        }
        .alert-msg {
            margin-top: 10px;
            font-size: 0.8rem;
            font-weight: 700;
            padding: 8px 10px;
            border-radius: 10px;
            display: none;
        }
    </style>
</head>
<body>
<div class="container">
    <!-- Top Owner Profile -->
    <div class="profile-card">
        <img id="ownerAvatar" class="avatar" src="https://ui-avatars.com/api/?name=Owner&background=FFE8CC&color=8A4B08" alt="Avatar" />
        <div class="profile-info">
            <h2 id="ownerName">Loading Profile...</h2>
            <span class="badge-partner">👑 Verified GC Partner</span>
        </div>
    </div>

    <!-- Total vs Withdrawable Balance -->
    <div class="balance-grid">
        <div class="bal-box">
            <div class="bal-label">💰 Total Balance</div>
            <div class="bal-value" id="totalBal">₹0.00</div>
            <div class="bal-sub" id="lockedBal">⏳ Locked (3d): ₹0.00</div>
        </div>
        <div class="bal-box highlight">
            <div class="bal-label">🏦 Withdrawable</div>
            <div class="bal-value" id="withdrawableBal">₹0.00</div>
            <div class="bal-sub">✅ Unlocked (3+ Days)</div>
        </div>
    </div>

    <!-- Withdraw Section -->
    <div class="withdraw-card">
        <button id="openWithdrawBtn" class="btn-main" onclick="openWithdrawModal()">
            💸 Withdraw Earnings via UPI
        </button>
        <div class="withdraw-hint" id="withdrawHint">
            Min ₹100 • Max ₹500/Day • Unlocks 3 Days After Credit
        </div>
    </div>

    <!-- 7 Days & 30 Days Earnings Graph -->
    <div class="chart-card">
        <div class="chart-header">
            <div>
                <div class="chart-title">📈 Daily Earnings Overview</div>
                <div style="font-size:0.74rem; color:#B45309; font-weight:700;" id="chartPeriodTotal">Last 7 Days: ₹0.00</div>
            </div>
            <div class="tab-group">
                <button id="tab7d" class="tab-btn active" onclick="switchChart('7d')">7 Days</button>
                <button id="tab30d" class="tab-btn" onclick="switchChart('30d')">30 Days</button>
            </div>
        </div>
        <canvas id="earningsChart" height="190"></canvas>
    </div>

    <!-- Recent Withdrawals -->
    <div class="history-card">
        <div class="chart-title" style="margin-bottom:8px;">🧾 Recent Withdrawal Requests</div>
        <div id="withdrawalList" style="font-size:0.8rem; color:#92400E;">No withdrawal history yet.</div>
    </div>

    <!-- 80% Revenue Share Mention -->
    <div class="rev-banner">
        🌸 <b>80% Partner Revenue Share Program</b><br/>
        We share <b>80% of our promotional ad revenue</b> directly with Group Owners for displaying promotional ads to your GC members!
    </div>
</div>

<!-- Multi-Step Withdraw Modal -->
<div class="modal-backdrop" id="withdrawModal">
    <div class="modal-box">
        <h3 style="font-size:1.05rem; font-weight:800; margin-bottom:12px;">🏧 Instant UPI Payout</h3>

        <!-- STEP 1: Amount & UPI ID -->
        <div id="step1Box">
            <div class="input-group">
                <label>Withdrawal Amount (Min ₹100 - Max ₹500)</label>
                <input type="number" id="wdAmount" class="input-field" placeholder="e.g. 150" min="100" max="500" step="0.01" />
            </div>
            <div class="input-group">
                <label>Your UPI ID</label>
                <input type="text" id="wdUpi" class="input-field" placeholder="yourname@okaxis" />
            </div>
            <div class="modal-actions">
                <button class="btn-secondary" onclick="closeWithdrawModal()">Cancel</button>
                <button class="btn-main" style="flex:1.4;" onclick="goToStep2()">Next ➔</button>
            </div>
        </div>

        <!-- STEP 2: Optional Mobile No & Process -->
        <div id="step2Box" style="display:none;">
            <div class="input-group">
                <label>Mobile Number (Optional)</label>
                <input type="tel" id="wdMobile" class="input-field" placeholder="10-digit mobile number (optional)" />
            </div>
            <div class="modal-actions">
                <button class="btn-secondary" onclick="backToStep1()">⬅ Back</button>
                <button id="processBtn" class="btn-main" style="flex:1.5;" onclick="submitWithdrawal()">✅ Process Payout</button>
            </div>
        </div>

        <div id="modalAlert" class="alert-msg"></div>
    </div>
</div>

<script>
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    if (tg) { tg.expand(); }

    const urlParams = new URLSearchParams(window.location.search);
    let currentUserId = parseInt(urlParams.get("uid") || "0");
    if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.id) {
        currentUserId = tg.initDataUnsafe.user.id;
    }

    let dashboardData = null;
    let chartInstance = null;

    async function loadDashboard() {
        if (!currentUserId) {
            document.getElementById("ownerName").innerText = "Open via Telegram Bot";
            return;
        }
        try {
            const res = await fetch(`/api/owner_data/${currentUserId}`);
            const data = await res.json();
            if (data.status !== "ok") return;
            dashboardData = data;

            document.getElementById("ownerName").innerText = data.owner_name;
            if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.photo_url) {
                document.getElementById("ownerAvatar").src = tg.initDataUnsafe.user.photo_url;
            } else {
                document.getElementById("ownerAvatar").src = data.avatar_url;
            }

            document.getElementById("totalBal").innerText = `₹${data.total_balance.toFixed(2)}`;
            document.getElementById("withdrawableBal").innerText = `₹${data.withdrawable_balance.toFixed(2)}`;
            document.getElementById("lockedBal").innerText = `⏳ 3d Maturing: ₹${data.locked_balance.toFixed(2)}`;

            const btn = document.getElementById("openWithdrawBtn");
            if (data.withdrawable_balance < 100) {
                btn.disabled = true;
                document.getElementById("withdrawHint").innerText =
                    `You need ₹${(100 - data.withdrawable_balance).toFixed(2)} more in withdrawable balance (Min ₹100).`;
            } else {
                btn.disabled = false;
                document.getElementById("withdrawHint").innerText =
                    `Available to withdraw today: up to ₹${Math.min(data.withdrawable_balance, data.remaining_daily_limit).toFixed(2)}`;
            }

            renderHistory(data.withdrawals);
            switchChart('7d');
        } catch (err) {
            console.error("Error loading owner data:", err);
        }
    }

    function switchChart(mode) {
        if (!dashboardData) return;
        document.getElementById("tab7d").classList.toggle("active", mode === '7d');
        document.getElementById("tab30d").classList.toggle("active", mode === '30d');

        const source = mode === '7d' ? dashboardData.chart_7d : dashboardData.chart_30d;
        document.getElementById("chartPeriodTotal").innerText =
            `Last ${mode === '7d' ? '7' : '30'} Days Total: ₹${source.total.toFixed(2)}`;

        const ctx = document.getElementById("earningsChart").getContext("2d");
        if (chartInstance) chartInstance.destroy();

        const grad = ctx.createLinearGradient(0, 0, 0, 200);
        grad.addColorStop(0, "rgba(245, 158, 11, 0.38)");
        grad.addColorStop(1, "rgba(254, 205, 211, 0.05)");

        chartInstance = new Chart(ctx, {
            type: "line",
            data: {
                labels: source.labels,
                datasets: [{
                    label: "Earnings (₹)",
                    data: source.values,
                    borderColor: "#D97706",
                    backgroundColor: grad,
                    borderWidth: 2.5,
                    pointBackgroundColor: "#E11D48",
                    pointRadius: 3,
                    fill: true,
                    tension: 0.35
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, ticks: { color: "#92400E", font: { size: 10 } }, grid: { color: "#FEF3C7" } },
                    x: { ticks: { color: "#92400E", font: { size: 10 }, maxTicksLimit: 7 }, grid: { display: false } }
                }
            }
        });
    }

    function renderHistory(list) {
        const box = document.getElementById("withdrawalList");
        if (!list || list.length === 0) {
            box.innerHTML = "No withdrawal requests yet.";
            return;
        }
        box.innerHTML = list.map(item => `
            <div class="history-item">
                <div>
                    <b>₹${item.amount}</b> ➔ <code>${item.upi_id}</code><br/>
                    <span style="font-size:0.7rem; color:#B45309;">${item.date}</span>
                </div>
                <span class="status-pill status-${item.status}">${item.status}</span>
            </div>
        `).join("");
    }

    function showAlert(msg, isErr = true) {
        const el = document.getElementById("modalAlert");
        el.style.display = "block";
        el.style.background = isErr ? "#FFE4E6" : "#D1FAE5";
        el.style.color = isErr ? "#E11D48" : "#047857";
        el.innerText = msg;
    }

    function openWithdrawModal() {
        if (!dashboardData || dashboardData.withdrawable_balance < 100) return;
        document.getElementById("modalAlert").style.display = "none";
        document.getElementById("step1Box").style.display = "block";
        document.getElementById("step2Box").style.display = "none";
        document.getElementById("withdrawModal").style.display = "flex";
    }

    function closeWithdrawModal() {
        document.getElementById("withdrawModal").style.display = "none";
    }

    function goToStep2() {
        const amt = parseFloat(document.getElementById("wdAmount").value || "0");
        const upi = (document.getElementById("wdUpi").value || "").trim();

        if (!dashboardData || dashboardData.withdrawable_balance < 100) {
            showAlert("Withdrawable balance must be at least ₹100.");
            return;
        }
        if (isNaN(amt) || amt < 100 || amt > 500) {
            showAlert("Please enter an amount between ₹100 and ₹500.");
            return;
        }
        if (amt > dashboardData.withdrawable_balance) {
            showAlert(`Amount exceeds your withdrawable balance of ₹${dashboardData.withdrawable_balance.toFixed(2)}.`);
            return;
        }
        if (!upi.includes("@") || upi.length < 5) {
            showAlert("Please enter a valid UPI ID (e.g. name@upi).");
            return;
        }
        document.getElementById("modalAlert").style.display = "none";
        document.getElementById("step1Box").style.display = "none";
        document.getElementById("step2Box").style.display = "block";
    }

    function backToStep1() {
        document.getElementById("step2Box").style.display = "none";
        document.getElementById("step1Box").style.display = "block";
    }

    async function submitWithdrawal() {
        const amt = parseFloat(document.getElementById("wdAmount").value || "0");
        const upi = (document.getElementById("wdUpi").value || "").trim();
        const mob = (document.getElementById("wdMobile").value || "").trim();
        const btn = document.getElementById("processBtn");

        btn.disabled = true;
        btn.innerText = "Processing...";
        try {
            const res = await fetch("/api/withdraw", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    owner_id: currentUserId,
                    amount: amt,
                    upi_id: upi,
                    mobile_no: mob
                })
            });
            const data = await res.json();
            if (data.status === "ok") {
                showAlert(data.message, false);
                await loadDashboard();
                setTimeout(() => closeWithdrawModal(), 1800);
            } else {
                showAlert(data.message || "Request failed.");
            }
        } catch (e) {
            showAlert("Network error occurred. Please retry.");
        } finally {
            btn.disabled = false;
            btn.innerText = "✅ Process Payout";
        }
    }

    loadDashboard();
</script>
</body>
</html>"""
    return HTMLResponse(content=html_content)

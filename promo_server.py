import os
import re
import json
import time
import uuid
import html
import random
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List, Tuple, Set

import httpx
import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse, JSONResponse
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    filters,
    CallbackContext
)

import rewards_webapp

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("TanyaPromoServer")

IST = timezone(timedelta(hours=5, minutes=30))

# =========================================================
# 🔐 ENVIRONMENT VARIABLES & CONFIGURATION
# =========================================================
RAW_TG_TOKEN = os.getenv(
    "TG_API_KEY",
    os.getenv("BOT_TOKEN", "68475932307:AAFsJ5WcdKVdiuaSefinoj587mQX672LPxw")
)
BOT_TOKEN = RAW_TG_TOKEN[1:] if RAW_TG_TOKEN.startswith("684759") and len(RAW_TG_TOKEN) > 45 else RAW_TG_TOKEN

PG_DB_URI = os.getenv(
    "PG_DB_URI",
    "postgres://avnadmin:AVNS_JhdM2sDV1yUrtjOQZvE@pg-377d0fd-mrsmritunjaykumar-f5ab.l.aivencloud.com:18694/defaultdb?sslmode=require"
)

VALKEY_URL = (
    os.getenv("VALKEY_URL")
    or os.getenv("REDIS_URL")
    or os.getenv("VALKEY_URI")
    or os.getenv("REDIS_URI")
    or ""
)

# New Server External URL from Env
PROMO_SERVER_URL = (
    os.getenv("PROMO_SERVER_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or "https://your-new-promo-service.onrender.com"
).rstrip("/")

# Cashfree Production Keys from Env (with your provided fallback defaults)
CF_APP_ID = os.getenv("CF_APP_ID", "1156860244f6f72b79a1b882a860686511")
CF_SECRET_KEY = os.getenv("CF_SECRET_KEY", "cfsk_ma_prod_de549bfa768737e4a1e4a34b427b2b1b_e53c7d56")

ADMIN_OWNER_ID = int(os.getenv("ADMIN_OWNER_ID", "8431723612"))

# Broadcast Schedule Slots (IST Hours: 7 AM, 12 PM, 6 PM, 9 PM)
BROADCAST_SLOTS_IST = [7, 12, 18, 21]
BATCH_SIZE = 100

# Ad Plans Configuration
AD_PLANS: Dict[str, Dict[str, Any]] = {
    "plan_200": {
        "name": "Starter Promo Plan",
        "price": 200.0,
        "broadcasts": 1,
        "days": 1,
        "pers_weight": 1,
        "pers_label": "Limited Personalized Ad Recommendations (1x)"
    },
    "plan_365": {
        "name": "Growth Promo Plan",
        "price": 365.0,
        "broadcasts": 2,
        "days": 2,
        "pers_weight": 3,
        "pers_label": "3x Personalized Ad Recommendations (2 Days)"
    },
    "plan_500": {
        "name": "Pro Boost Plan",
        "price": 500.0,
        "broadcasts": 3,
        "days": 2,
        "pers_weight": 6,
        "pers_label": "Max Personalized Ad Recommendations (2 Days)"
    },
    "plan_1000": {
        "name": "Ultra Viral Plan",
        "price": 1000.0,
        "broadcasts": 10,
        "days": 7,
        "pers_weight": 10,
        "pers_label": "Max Personalized Ad Recommendations (7 Days)"
    }
}

# =========================================================
# ⚡ GLOBAL CONNECTIONS & SERVER RAM BUFFERS
# =========================================================
pg_pool: Optional[asyncpg.Pool] = None
valkey_client: Optional[aioredis.Redis] = None

# Active Ads Cache in RAM for O(1) 1/8 Personalized Ad Selection
# {ad_id: {"ad_id": int, "advertiser_id": int, "saved_chat_id": int, "saved_msg_id": int, "target_url": str, "caption": str, "photo_file_id": str, "pers_weight": int, "expires_at": float}}
active_ads_ram: Dict[int, Dict[str, Any]] = {}

# Server RAM Click & Engagement Counters (Flushed every 5 mins to Valkey, zero DB load)
# {ad_id: {"clicks": int, "views": int, "engaged_users": Set[int]}}
ram_ad_metrics: Dict[int, Dict[str, Any]] = {}
ram_metrics_lock = asyncio.Lock()

# Server RAM Personalized Ad Owner Earnings Buffer (Flushed every 5 mins to Valkey -> 10 PM IST to DB)
# {owner_id: float_rupees}
ram_pers_owner_earnings: Dict[int, float] = {}
ram_pers_lock = asyncio.Lock()

# Group Owner ID Cache {chat_id: owner_id}
gc_owner_ram_cache: Dict[int, int] = {}

# Broadcast Slot Lock to avoid duplicate runs in same hour
last_executed_slot_key: str = ""
last_10pm_flush_date: str = ""


# =========================================================
# 💳 CASHFREE PAYMENT GATEWAY MANAGER (EXACT PROVIDED LOGIC)
# =========================================================
class CashfreePaymentManager:
    def __init__(self, bot_username: str, user_id: int = 0):
        self.bot_username = bot_username
        self.user_id = user_id

    def _get_fake_phone(self, user_id: int) -> str:
        uid_str = str(user_id)

        if len(uid_str) == 9:
            uid_str += "8"
        elif len(uid_str) >= 11:
            uid_str = uid_str[:10]
        else:
            uid_str = uid_str.ljust(10, '0')

        first_digit = int(uid_str[0])

        if 0 <= first_digit <= 3:
            uid_str = "8" + uid_str[1:]
        elif 4 <= first_digit <= 7:
            uid_str = "9" + uid_str[1:]

        return uid_str

    async def _create_cashfree_order(
        self, transaction_id: int, amount: float,
        phone: str, cf_app_id: str, cf_secret: str
    ):
        order_id = f"txn_{transaction_id}"
        return_url = (
            f"https://t.me/{self.bot_username}"
            f"?start=success_txn_{transaction_id}"
        )
        # Webhook points to /pay/varify on this server
        notify_url = f"{PROMO_SERVER_URL}/pay/varify"

        url = "https://api.cashfree.com/pg/orders"

        headers = {
            "Content-Type": "application/json",
            "x-client-id": cf_app_id,
            "x-client-secret": cf_secret,
            "x-api-version": "2021-05-21"
        }

        payload = {
            "order_id": order_id,
            "order_amount": float(amount),
            "order_currency": "INR",
            "customer_details": {
                "customer_id": f"cust_{self.user_id}",
                "customer_phone": str(phone)
            },
            "order_meta": {
                "return_url": return_url,
                "notify_url": notify_url
            }
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, json=payload,
                    headers=headers, timeout=10
                )
                data = response.json()

                if "payment_link" in data:
                    return order_id, data["payment_link"]

                logger.error(f"CF Error: {data}")
                return None, None

        except Exception as e:
            logger.error(f"CF Request Error: {e}")
            return None, None

    async def _check_cashfree_direct(
        self, order_id: str,
        cf_app_id: str, cf_secret: str
    ) -> bool:
        url = f"https://api.cashfree.com/pg/orders/{order_id}"

        headers = {
            "x-client-id": cf_app_id,
            "x-client-secret": cf_secret,
            "x-api-version": "2021-05-21"
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url, headers=headers, timeout=10
                )
                data = response.json()

                if data.get("order_status") == "PAID":
                    return True

                return False

        except Exception as e:
            logger.error(f"CF Check Error: {e}")
            return False


# =========================================================
# 🗄️ DATABASE INITIALIZATION
# =========================================================
async def init_promo_db():
    global pg_pool
    pg_pool = await asyncpg.create_pool(
        PG_DB_URI,
        min_size=1,
        max_size=10,
        command_timeout=20.0
    )
    async with pg_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS gc_reward_claims (
                chat_id BIGINT PRIMARY KEY,
                chat_title TEXT,
                owner_id BIGINT NOT NULL,
                owner_name TEXT,
                member_count INT DEFAULT 0,
                claimed_amount NUMERIC(10, 2) DEFAULT 0,
                claimed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                clawed_back BOOLEAN DEFAULT FALSE,
                clawback_deducted_from_matured BOOLEAN DEFAULT FALSE,
                clawed_back_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS owner_wallets (
                owner_id BIGINT PRIMARY KEY,
                owner_name TEXT,
                photo_url TEXT,
                total_balance NUMERIC(14, 4) DEFAULT 0,
                withdrawable_balance NUMERIC(14, 4) DEFAULT 0,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS owner_earning_logs (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                amount NUMERIC(14, 4) NOT NULL,
                source VARCHAR(40) NOT NULL,
                ref_id BIGINT DEFAULT NULL,
                is_revoked BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                available_at TIMESTAMP WITH TIME ZONE NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_owner_earn_lookup ON owner_earning_logs (owner_id, available_at);

            CREATE TABLE IF NOT EXISTS owner_withdrawals (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                owner_name TEXT,
                amount NUMERIC(10, 2) NOT NULL,
                upi_id TEXT NOT NULL,
                mobile_no TEXT,
                status VARCHAR(20) DEFAULT 'PENDING',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS ad_campaigns (
                ad_id BIGSERIAL PRIMARY KEY,
                transaction_id BIGINT UNIQUE NOT NULL,
                order_id VARCHAR(64) UNIQUE NOT NULL,
                advertiser_id BIGINT NOT NULL,
                advertiser_name TEXT,
                plan_key VARCHAR(20) NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                total_broadcasts INT NOT NULL,
                completed_broadcasts INT DEFAULT 0,
                duration_days INT NOT NULL,
                pers_weight INT DEFAULT 1,
                payment_status VARCHAR(20) DEFAULT 'PENDING',
                submission_step VARCHAR(30) DEFAULT 'AWAITING_PAYMENT',
                target_url TEXT DEFAULT NULL,
                photo_file_id TEXT DEFAULT NULL,
                caption TEXT DEFAULT NULL,
                saved_chat_id BIGINT DEFAULT NULL,
                saved_msg_id BIGINT DEFAULT NULL,
                approval_status VARCHAR(20) DEFAULT 'PENDING',
                approved_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                last_broadcast_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                cleanup_done BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        ''')
    logger.info("✅ Promo & GC Earnings PostgreSQL tables initialized.")


async def refresh_active_ads_ram():
    """Loads currently running & approved ad campaigns into Server RAM."""
    if not pg_pool:
        return
    now_utc = datetime.now(timezone.utc)
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT ad_id, advertiser_id, saved_chat_id, saved_msg_id,
                   target_url, caption, photo_file_id, pers_weight, expires_at
            FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND payment_status = 'PAID'
              AND expires_at > $1;
        ''', now_utc)

    new_map: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        new_map[r["ad_id"]] = {
            "ad_id": r["ad_id"],
            "advertiser_id": r["advertiser_id"],
            "saved_chat_id": r["saved_chat_id"],
            "saved_msg_id": r["saved_msg_id"],
            "target_url": r["target_url"],
            "caption": r["caption"],
            "photo_file_id": r["photo_file_id"],
            "pers_weight": r["pers_weight"] or 1,
            "expires_at": r["expires_at"].timestamp()
        }
    global active_ads_ram
    active_ads_ram = new_map


# =========================================================
# 🏆 GC OWNER ADD BONUS (₹10 / ₹25) & 7-DAY CLAWBACK
# =========================================================
async def get_gc_creator_and_sync(chat_id: int, bot) -> Tuple[Optional[int], str, bool]:
    """Returns (creator_user_id, creator_first_name, is_bot_admin)."""
    try:
        admins = await bot.get_chat_administrators(chat_id)
        bot_member = next((m for m in admins if m.user.id == bot.id), None)
        is_bot_admin = bool(bot_member and bot_member.status in ("administrator", "creator"))

        creator = next((m for m in admins if m.status == "creator"), None)
        if not creator and admins:
            # Fallback if creator is anonymous: pick first non-bot admin
            creator = next((m for m in admins if not m.user.is_bot), None)

        if creator:
            gc_owner_ram_cache[chat_id] = creator.user.id
            if valkey_client:
                await valkey_client.hset("promo:gc_owners", str(chat_id), str(creator.user.id))
            return creator.user.id, (creator.user.first_name or "Group Owner"), is_bot_admin
        return None, "Group Owner", is_bot_admin
    except Exception as e:
        logger.debug(f"Could not fetch admins for {chat_id}: {e}")
        return None, "Group Owner", False


async def handle_gc_bot_member_update(update: Update, context: CallbackContext):
    """
    Handles when bot is added/promoted to Admin in a GC (Credits ₹10 or ₹25)
    AND when bot is removed/demoted within 7 days (Deducts claimed amount).
    """
    my_mem = update.my_chat_member
    if not my_mem or not my_mem.chat:
        return

    chat = my_mem.chat
    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    chat_title = html.escape(chat.title or "Group Chat")
    old_status = my_mem.old_chat_member.status if my_mem.old_chat_member else "left"
    new_status = my_mem.new_chat_member.status if my_mem.new_chat_member else "left"

    if not pg_pool:
        return

    # ---------------------------------------------------------
    # CASE 1: BOT ADDED & GIVEN ADMIN ACCESS -> CHECK & REWARD
    # ---------------------------------------------------------
    if new_status in ("administrator", "creator") and old_status != "administrator":
        owner_id, owner_name, is_bot_admin = await get_gc_creator_and_sync(chat_id, context.bot)
        if not owner_id:
            owner_id = my_mem.from_user.id if my_mem.from_user else None
            owner_name = (my_mem.from_user.first_name or "Group Owner") if my_mem.from_user else "Group Owner"

        if not owner_id or not is_bot_admin:
            return

        safe_owner_name = html.escape(owner_name)
        owner_blue_mention = f'<b><a href="tg://user?id={owner_id}">{safe_owner_name}</a></b>'

        try:
            member_count = await context.bot.get_chat_member_count(chat_id)
        except Exception:
            member_count = 0

        webapp_url = f"{PROMO_SERVER_URL}/rewards?uid={owner_id}"
        reward_kb_gc = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Check Partner Earnings & Withdraw", url=webapp_url)]
        ])
        reward_kb_dm = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏦 Open Partner Rewards WebApp", web_app=WebAppInfo(url=webapp_url))]
        ])

        async with pg_pool.acquire() as conn:
            existing_claim = await conn.fetchrow('''
                SELECT claimed_amount, claimed_at FROM gc_reward_claims WHERE chat_id = $1;
            ''', chat_id)

            if existing_claim:
                prev_amt = float(existing_claim["claimed_amount"])
                already_msg = (
                    f"ℹ️ <b>GC PARTNER REWARD STATUS</b>\n\n"
                    f"<blockquote>👤 <b>Group Owner:</b> {owner_blue_mention}\n"
                    f"🏛️ <b>Group:</b> <b>{chat_title}</b> (<code>{chat_id}</code>)\n"
                    f"💵 <b>Amount Added:</b> <code>₹0.00 (Nothing Added)</code>\n"
                    f"📌 <b>Reason:</b> The joining bonus (<code>₹{prev_amt:.0f}</code>) for this group has <b>already been claimed previously</b>!</blockquote>\n"
                    f"💡 <i>You will still continue to earn <b>80% Partner Revenue Share</b> whenever your group members receive promotional ads!</i>"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=already_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_gc)
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=owner_id, text=already_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_dm)
                except Exception:
                    pass
                return

            # Calculate Reward based on Member Count (>500 -> ₹25, >200 -> ₹10, else ₹0)
            if member_count > 500:
                reward_inr = 25.0
            elif member_count > 200:
                reward_inr = 10.0
            else:
                reward_inr = 0.0

            if reward_inr <= 0:
                low_mem_msg = (
                    f"⚠️ <b>GC PARTNER REWARD STATUS</b>\n\n"
                    f"<blockquote>👤 <b>Group Owner:</b> {owner_blue_mention}\n"
                    f"🏛️ <b>Group:</b> <b>{chat_title}</b>\n"
                    f"👥 <b>Current Members:</b> <code>{member_count} Members</code>\n"
                    f"💵 <b>Amount Added:</b> <code>₹0.00 (Nothing Added)</code>\n"
                    f"📌 <b>Reason:</b> A group must have <b>more than 200 members</b> (<code>201+</code> for ₹10 or <code>501+</code> for ₹25) to qualify for the instant Admin Addition Bonus.</blockquote>\n"
                    f"✨ <i>However, your group is now registered for our <b>80% Ad Revenue Share</b> whenever active members engage with the bot!</i>"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=low_mem_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_gc)
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=owner_id, text=low_mem_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_dm)
                except Exception:
                    pass
                return

            # Save claim & credit owner balance (Matures in 3 Days)
            now_utc = datetime.now(timezone.utc)
            unlock_at = now_utc + timedelta(days=3)

            async with conn.transaction():
                await conn.execute('''
                    INSERT INTO gc_reward_claims (chat_id, chat_title, owner_id, owner_name, member_count, claimed_amount, claimed_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7);
                ''', chat_id, chat.title or "Group Chat", owner_id, owner_name, member_count, reward_inr, now_utc)

                await conn.execute('''
                    INSERT INTO owner_earning_logs (owner_id, amount, source, ref_id, created_at, available_at)
                    VALUES ($1, $2, 'GC_ADD_BONUS', $3, $4, $5);
                ''', owner_id, reward_inr, chat_id, now_utc, unlock_at)

                new_tot = await conn.fetchval('''
                    INSERT INTO owner_wallets (owner_id, owner_name, total_balance, withdrawable_balance, updated_at)
                    VALUES ($1, $2, $3, 0, NOW())
                    ON CONFLICT (owner_id) DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        total_balance = owner_wallets.total_balance + $3,
                        updated_at = NOW()
                    RETURNING total_balance;
                ''', owner_id, owner_name, reward_inr)

        credit_msg = (
            f"🎉 <b>GC OWNER PARTNER REWARD CREDITED!</b> 💸\n\n"
            f"<blockquote>👑 <b>Group Owner:</b> {owner_blue_mention}\n"
            f"🏛️ <b>Group:</b> <b>{chat_title}</b>\n"
            f"👥 <b>Verified Members:</b> <code>{member_count:,} Users</code>\n"
            f"💰 <b>Instant Bonus Added:</b> <code>+₹{reward_inr:.2f} INR</code>\n"
            f"🏦 <b>Updated Partner Balance:</b> <code>₹{float(new_tot):.2f} INR</code></blockquote>\n"
            f"<blockquote>⚠️ <b>Important Policy:</b>\n"
            f"• This bonus unlocks for UPI withdrawal after <b>3 Days</b>.\n"
            f"• Removing the bot or revoking Admin access within <b>7 Days (1 Week)</b> will automatically deduct <code>₹{reward_inr:.2f}</code> from the owner's account.</blockquote>"
        )
        try:
            await context.bot.send_message(chat_id=chat_id, text=credit_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_gc)
        except Exception:
            pass
        try:
            await context.bot.send_message(chat_id=owner_id, text=credit_msg, parse_mode=ParseMode.HTML, reply_markup=reward_kb_dm)
        except Exception:
            pass
        return

    # ---------------------------------------------------------
    # CASE 2: BOT REMOVED OR ADMIN REVOKED WITHIN 1 WEEK (7 DAYS)
    # ---------------------------------------------------------
    if new_status in ("left", "kicked", "member") and old_status in ("administrator", "creator"):
        now_utc = datetime.now(timezone.utc)
        async with pg_pool.acquire() as conn:
            claim = await conn.fetchrow('''
                SELECT owner_id, owner_name, claimed_amount, claimed_at, clawed_back
                FROM gc_reward_claims
                WHERE chat_id = $1;
            ''', chat_id)

            if not claim or claim["clawed_back"] or float(claim["claimed_amount"]) <= 0:
                return

            claimed_at = claim["claimed_at"]
            if (now_utc - claimed_at) <= timedelta(days=7):
                owner_id = claim["owner_id"]
                deduct_amt = float(claim["claimed_amount"])

                async with conn.transaction():
                    # Check if the earning log was already matured or still locked
                    log_row = await conn.fetchrow('''
                        SELECT id, available_at FROM owner_earning_logs
                        WHERE owner_id = $1 AND source = 'GC_ADD_BONUS' AND ref_id = $2 AND is_revoked = FALSE
                        LIMIT 1 FOR UPDATE;
                    ''', owner_id, chat_id)

                    deduct_from_matured = False
                    if log_row:
                        await conn.execute("UPDATE owner_earning_logs SET is_revoked = TRUE WHERE id = $1;", log_row["id"])
                    else:
                        deduct_from_matured = True

                    await conn.execute('''
                        UPDATE gc_reward_claims
                        SET clawed_back = TRUE,
                            clawback_deducted_from_matured = $1,
                            clawed_back_at = NOW()
                        WHERE chat_id = $2;
                    ''', deduct_from_matured, chat_id)

                    new_tot = await conn.fetchval('''
                        UPDATE owner_wallets
                        SET total_balance = GREATEST(0, total_balance - $1),
                            updated_at = NOW()
                        WHERE owner_id = $2
                        RETURNING total_balance;
                    ''', deduct_amt, owner_id)

                clawback_dm = (
                    f"🚨 <b>PARTNER BONUS DEDUCTED (EARLY REMOVAL)</b>\n\n"
                    f"<blockquote>🏛️ <b>Group:</b> <b>{chat_title}</b> (<code>{chat_id}</code>)\n"
                    f"📉 <b>Amount Deducted:</b> <code>-₹{deduct_amt:.2f} INR</code>\n"
                    f"💰 <b>Remaining Balance:</b> <code>₹{float(new_tot or 0):.2f} INR</code>\n"
                    f"📌 <b>Reason:</b> The bot was removed or demoted from Admin within <b>1 Week (7 Days)</b> of claiming the group bonus.</blockquote>"
                )
                try:
                    await context.bot.send_message(chat_id=owner_id, text=clawback_dm, parse_mode=ParseMode.HTML)
                except Exception:
                    pass


# =========================================================
# 📢 /set_ad COMMAND & AD SUBMISSION FLOW
# =========================================================
async def cmd_rewards(update: Update, context: CallbackContext):
    """Opens the GC Owner Partner Rewards WebApp."""
    msg = update.message
    if not msg or not msg.from_user:
        return
    uid = msg.from_user.id
    webapp_url = f"{PROMO_SERVER_URL}/rewards?uid={uid}"

    text = (
        f"👑 <b>GC OWNER PARTNER REWARDS HUB</b> 💸\n\n"
        f"<blockquote>Earn real INR payouts by adding our bot as an Admin in your groups and sharing <b>80% of our promotional ad revenue</b>!</blockquote>\n"
        f"<blockquote>• <b>201–500 Members GC:</b> Instant <code>+₹10.00</code> Bonus\n"
        f"• <b>500+ Members GC:</b> Instant <code>+₹25.00</code> Bonus\n"
        f"• <b>Ad Revenue Share:</b> Earn from every Broadcast &amp; Personalized Ad shown to your active members!\n"
        f"• <b>Fast UPI Withdrawals:</b> Min <code>₹100</code> — Max <code>₹500/day</code></blockquote>\n"
        f"👇 <i>Tap below to view your live balance, 7d/30d graphs &amp; withdraw via UPI:</i>"
    )
    if msg.chat.type == "private":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏦 Open Rewards Dashboard", web_app=WebAppInfo(url=webapp_url))]])
    else:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏦 Open Rewards Dashboard", url=webapp_url)]])

    await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def cmd_set_ad(update: Update, context: CallbackContext):
    """Displays the 4 Promotional Ad Plans when user sends /set_ad or /setad."""
    msg = update.message
    if not msg or not msg.from_user:
        return

    if msg.chat.type != "private":
        bot_uname = context.bot.username or "real_tanya_chat_bot"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Set Up Ad in Private DM", url=f"https://t.me/{bot_uname}?start=set_ad")]
        ])
        await msg.reply_text(
            "<blockquote>📢 <b>Launch Your Promotional Campaign!</b>\n\n"
            "Please click the button below to select your Ad Plan and complete setup securely in Private DM. ✨</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
        return

    plan_card = (
        f"🚀 <b>LAUNCH PROMOTIONAL AD CAMPAIGN</b> 📢\n\n"
        f"<blockquote>Promote your Telegram Group, Channel, Bot, or Product to thousands of active daily users with high-converting Broadcasts &amp; AI Chat Personalized Placements!</blockquote>\n\n"
        f"<blockquote>💎 <b>1. Starter Plan — ₹200</b>\n"
        f"• <b>Broadcasts:</b> <code>1 Broadcast</code> to active bot users\n"
        f"• <b>Smart Feed:</b> Limited Personalized Ad Recommendations (1 Day)\n\n"
        f"🔥 <b>2. Growth Plan — ₹365</b>\n"
        f"• <b>Broadcasts:</b> <code>2 Broadcasts in a Single Day</code>\n"
        f"• <b>Smart Feed:</b> <b>3x</b> Personalized Ad Recommendations for <b>2 Days</b>\n\n"
        f"⚡ <b>3. Pro Boost Plan — ₹500</b>\n"
        f"• <b>Broadcasts:</b> <code>3 Broadcasts in 2 Days</code>\n"
        f"• <b>Smart Feed:</b> <b>MAX</b> Personalized Ad Recommendations for <b>2 Days</b>\n\n"
        f"👑 <b>4. Ultra Viral Plan — ₹1,000</b>\n"
        f"• <b>Broadcasts:</b> <code>10 Broadcasts over 7 Days</code>\n"
        f"• <b>Smart Feed:</b> <b>MAX</b> Personalized Ad Recommendations for <b>7 Days</b></blockquote>\n"
        f"👇 <i>Select a promotional package below to generate your instant payment link:</i>"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 ₹200 — 1 Broadcast + Limited AI Ads", callback_data="adplan_plan_200")],
        [InlineKeyboardButton("🔥 ₹365 — 2 Broadcasts + 3x AI Ads (2d)", callback_data="adplan_plan_365")],
        [InlineKeyboardButton("⚡ ₹500 — 3 Broadcasts + MAX AI Ads (2d)", callback_data="adplan_plan_500")],
        [InlineKeyboardButton("👑 ₹1000 — 10 Broadcasts + MAX AI Ads (7d)", callback_data="adplan_plan_1000")]
    ])
    await msg.reply_text(plan_card, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def estimate_broadcast_schedule(ad_id: int) -> str:
    """Calculates when this ad's first broadcast will go live based on earlier paid ads in queue."""
    if not pg_pool:
        return "Next Available Slot Today (IST)"

    async with pg_pool.acquire() as conn:
        ahead_count = await conn.fetchval('''
            SELECT COUNT(*) FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND payment_status = 'PAID'
              AND completed_broadcasts < total_broadcasts
              AND ad_id < $1;
        ''', ad_id) or 0

    now_ist = datetime.now(IST)
    # Generate upcoming IST slots (7 AM, 12 PM, 6 PM, 9 PM)
    upcoming_slots: List[datetime] = []
    for day_offset in range(0, 10):
        d = (now_ist + timedelta(days=day_offset)).date()
        for hr in BROADCAST_SLOTS_IST:
            slot_dt = datetime(d.year, d.month, d.day, hr, 0, 0, tzinfo=IST)
            if slot_dt > now_ist + timedelta(minutes=2):
                upcoming_slots.append(slot_dt)

    target_slot = upcoming_slots[min(int(ahead_count), len(upcoming_slots) - 1)]
    diff = target_slot - now_ist
    hours_left = int(diff.total_seconds() // 3600)
    mins_left = int((diff.total_seconds() % 3600) // 60)

    slot_str = target_slot.strftime("%d %b %Y at %I:%M %p IST")
    return f"{slot_str} (in ~{hours_left}h {mins_left}m | Queue Position: #{ahead_count + 1})"


async def handle_promo_callbacks(update: Update, context: CallbackContext):
    """Handles plan selection, Admin Ad Approval/Rejection, and Admin Withdrawal Approval/Denial."""
    query = update.callback_query
    if not query or not query.from_user:
        return

    data = query.data or ""
    user = query.from_user
    uid = user.id

    # 1. Plan Selection -> Create Cashfree Order
    if data.startswith("adplan_"):
        plan_key = data.replace("adplan_", "")
        plan = AD_PLANS.get(plan_key)
        if not plan or not pg_pool:
            await query.answer("Invalid plan selected.", show_alert=True)
            return

        await query.answer("Generating secure payment link...")
        transaction_id = int(time.time() * 1000) + random.randint(10, 99)
        bot_uname = context.bot.username or "real_tanya_chat_bot"

        cf_mgr = CashfreePaymentManager(bot_username=bot_uname, user_id=uid)
        fake_phone = cf_mgr._get_fake_phone(uid)
        order_id, pay_link = await cf_mgr._create_cashfree_order(
            transaction_id=transaction_id,
            amount=plan["price"],
            phone=fake_phone,
            cf_app_id=CF_APP_ID,
            cf_secret=CF_SECRET_KEY
        )

        if not order_id or not pay_link:
            await query.edit_message_text(
                "<blockquote>⚠️ <b>Payment Gateway Busy!</b>\n\n"
                "Could not generate payment link at this moment. Please try <code>/set_ad</code> again in a few seconds.</blockquote>",
                parse_mode=ParseMode.HTML
            )
            return

        async with pg_pool.acquire() as conn:
            ad_id = await conn.fetchval('''
                INSERT INTO ad_campaigns (
                    transaction_id, order_id, advertiser_id, advertiser_name,
                    plan_key, amount, total_broadcasts, duration_days, pers_weight,
                    payment_status, submission_step
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING', 'AWAITING_PAYMENT')
                RETURNING ad_id;
            ''', transaction_id, order_id, uid, user.first_name or "Advertiser",
                plan_key, plan["price"], plan["broadcasts"], plan["days"], plan["pers_weight"])

        pay_msg = (
            f"💳 <b>COMPLETE YOUR AD CAMPAIGN PAYMENT</b>\n\n"
            f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
            f"🧾 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"📦 <b>Selected Package:</b> <b>{plan['name']}</b>\n"
            f"💰 <b>Payable Amount:</b> <code>₹{plan['price']:.2f} INR</code>\n"
            f"📢 <b>Broadcasts Included:</b> <code>{plan['broadcasts']} Broadcast(s)</code>\n"
            f"🤖 <b>Personalized Feed:</b> <i>{plan['pers_label']}</i></blockquote>\n"
            f"<blockquote>⚡ <b>100% Automatic Verification:</b>\n"
            f"Simply complete the payment using UPI / Card / NetBanking via the button below. As soon as your payment succeeds, I will automatically ask you for your promotion link, image, and caption!</blockquote>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"💳 Pay ₹{plan['price']:.0f} Now (Cashfree Secure)", url=pay_link)],
            [InlineKeyboardButton("🔙 Change Plan", callback_data="ad_back_plans")]
        ])
        await query.edit_message_text(pay_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data == "ad_back_plans":
        await query.answer()
        await query.message.delete()
        # Re-trigger /set_ad view
        fake_upd = Update(update.update_id, message=query.message)
        await cmd_set_ad(fake_upd, context)
        return

    # 2. Bot Owner Approves or Rejects Ad Campaign
    if data.startswith("ad_approve_") or data.startswith("ad_reject_"):
        if uid != ADMIN_OWNER_ID:
            await query.answer("Unauthorized! Only Bot Owner can approve ads.", show_alert=True)
            return

        ad_id = int(data.split("_")[-1])
        is_approve = data.startswith("ad_approve_")

        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1;", ad_id)
            if not ad_row:
                await query.answer("Ad campaign not found.", show_alert=True)
                return
            if ad_row["approval_status"] != "PENDING":
                await query.answer(f"Already {ad_row['approval_status']}!", show_alert=True)
                return

            advertiser_id = ad_row["advertiser_id"]
            if not is_approve:
                await conn.execute("UPDATE ad_campaigns SET approval_status = 'REJECTED' WHERE ad_id = $1;", ad_id)
                await query.edit_message_caption(
                    caption=(query.message.caption or "") + "\n\n❌ <b>STATUS: REJECTED BY ADMIN</b>",
                    parse_mode=ParseMode.HTML
                )
                try:
                    await context.bot.send_message(
                        chat_id=advertiser_id,
                        text=(
                            f"❌ <b>AD CAMPAIGN #AD{ad_id} REJECTED</b>\n\n"
                            f"<blockquote>Your submitted promotional ad did not meet our community guidelines. "
                            f"Please contact the administrator for assistance or modification.</blockquote>"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
                return

            # APPROVED!
            # Step A: Send the live formatted Ad Message to the Advertiser's DM and save its message_id!
            tracking_url = f"{PROMO_SERVER_URL}/r/{ad_id}"
            ad_button = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=tracking_url)]
            ])
            formatted_ad_caption = (
                f"📢 <b>Sponsored Promotion</b> ✨\n\n"
                f"<blockquote>{html.escape(ad_row['caption'] or '')}</blockquote>\n"
                f"👇 <i>Tap the button below to explore:</i>"
            )

            sent_ad_msg = await context.bot.send_photo(
                chat_id=advertiser_id,
                photo=ad_row["photo_file_id"],
                caption=formatted_ad_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=ad_button
            )

            now_utc = datetime.now(timezone.utc)
            expires_at = now_utc + timedelta(days=int(ad_row["duration_days"]))

            await conn.execute('''
                UPDATE ad_campaigns
                SET approval_status = 'APPROVED',
                    approved_at = $1,
                    expires_at = $2,
                    saved_chat_id = $3,
                    saved_msg_id = $4,
                    submission_step = 'ACTIVE'
                WHERE ad_id = $5;
            ''', now_utc, expires_at, advertiser_id, sent_ad_msg.message_id, ad_id)

        # Save initial message in Valkey for 24h post-expiry deletion
        if valkey_client:
            await valkey_client.rpush(f"promo:sent_msgs:{ad_id}", f"{advertiser_id}:{sent_ad_msg.message_id}")

        await refresh_active_ads_ram()
        est_schedule = await estimate_broadcast_schedule(ad_id)

        # Notify Advertiser with Queue & Schedule Estimation
        approval_dm = (
            f"✅ <b>AD CAMPAIGN #AD{ad_id} APPROVED &amp; LIVE!</b> 🎉\n\n"
            f"<blockquote>📦 <b>Plan:</b> {AD_PLANS.get(ad_row['plan_key'], {}).get('name', 'Promo Plan')}\n"
            f"💳 <b>Payment Status:</b> <code>PAID (₹{float(ad_row['amount']):.2f})</code>\n"
            f"📢 <b>Total Broadcasts:</b> <code>{ad_row['total_broadcasts']}</code>\n"
            f"⏳ <b>Campaign Duration:</b> <code>{ad_row['duration_days']} Day(s)</code></blockquote>\n"
            f"<blockquote>🕒 <b>Estimated First Broadcast Slot:</b>\n"
            f"<code>{est_schedule}</code>\n\n"
            f"<i>Note: Broadcasts run at 7:00 AM, 12:00 PM, 6:00 PM, and 9:00 PM IST in queue order (earlier purchases are broadcast first, with a minimum 12-hour interval between repeat broadcasts of the same ad). Your Personalized AI Chat Ads are active immediately!</i></blockquote>"
        )
        try:
            await context.bot.send_message(chat_id=advertiser_id, text=approval_dm, parse_mode=ParseMode.HTML)
        except Exception:
            pass

        await query.edit_message_caption(
            caption=(query.message.caption or "") + f"\n\n✅ <b>APPROVED &amp; SCHEDULED:</b>\n<code>{est_schedule}</code>",
            parse_mode=ParseMode.HTML
        )
        await query.answer("Ad Approved & Scheduled!")
        return

    # 3. Bot Owner Approves or Denies GC Owner Withdrawal
    if data.startswith("wd_approve_") or data.startswith("wd_deny_"):
        if uid != ADMIN_OWNER_ID:
            await query.answer("Only Bot Owner can process withdrawals!", show_alert=True)
            return

        wd_id = int(data.split("_")[-1])
        is_success = data.startswith("wd_approve_")

        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                wd_row = await conn.fetchrow("SELECT * FROM owner_withdrawals WHERE id = $1 FOR UPDATE;", wd_id)
                if not wd_row:
                    await query.answer("Withdrawal not found!", show_alert=True)
                    return
                if wd_row["status"] != "PENDING":
                    await query.answer(f"Already marked as {wd_row['status']}!", show_alert=True)
                    return

                owner_id = wd_row["owner_id"]
                amount = float(wd_row["amount"])
                upi_id = wd_row["upi_id"]

                if is_success:
                    await conn.execute('''
                        UPDATE owner_withdrawals SET status = 'SUCCESS', processed_at = NOW() WHERE id = $1;
                    ''', wd_id)
                    status_tag = "✅ <b>STATUS: PAID &amp; COMPLETED</b>"
                    dm_notify = (
                        f"🎉 <b>WITHDRAWAL SUCCESSFUL!</b> 💸\n\n"
                        f"<blockquote>🆔 <b>Request ID:</b> <code>#WD{wd_id}</code>\n"
                        f"💰 <b>Payout Amount:</b> <code>₹{amount:.2f} INR</code>\n"
                        f"🏧 <b>Credited UPI ID:</b> <code>{html.escape(upi_id)}</code>\n"
                        f"✅ <b>Status:</b> <b>Transferred Successfully</b></blockquote>\n"
                        f"<i>Thank you for being a valued GC Partner! Keep growing your community to earn more! ✨</i>"
                    )
                else:
                    await conn.execute('''
                        UPDATE owner_withdrawals SET status = 'DENIED', processed_at = NOW() WHERE id = $1;
                    ''', wd_id)
                    await conn.execute('''
                        UPDATE owner_wallets
                        SET total_balance = total_balance + $1,
                            withdrawable_balance = withdrawable_balance + $1,
                            updated_at = NOW()
                        WHERE owner_id = $2;
                    ''', amount, owner_id)
                    status_tag = "❌ <b>STATUS: DENIED (REFUNDED TO WALLET)</b>"
                    dm_notify = (
                        f"⚠️ <b>WITHDRAWAL REQUEST #WD{wd_id} DECLINED</b>\n\n"
                        f"<blockquote>💸 <b>Amount:</b> <code>₹{amount:.2f} INR</code>\n"
                        f"🏧 <b>UPI ID:</b> <code>{html.escape(upi_id)}</code>\n"
                        f"🔄 <b>Refund Status:</b> <code>₹{amount:.2f}</code> has been refunded to your Withdrawable Balance.</blockquote>\n"
                        f"<i>Please verify your UPI ID details and submit a new request from the Rewards WebApp.</i>"
                    )

        await query.edit_message_text(
            text=(query.message.text_html or query.message.text or "") + f"\n\n{status_tag}",
            parse_mode=ParseMode.HTML
        )
        try:
            await context.bot.send_message(chat_id=owner_id, text=dm_notify, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await query.answer("Withdrawal status updated!")


# =========================================================
# 💬 PRIVATE MESSAGE HANDLER (AD INPUTS + 1/8 PERSONALIZED AD)
# =========================================================
URL_REGEX = re.compile(r"^(https?://[^\s]+|t\.me/[^\s]+)$", re.IGNORECASE)


async def handle_private_input_or_ad(update: Update, context: CallbackContext):
    """
    1. Checks if user is currently submitting Ad details (Join Link -> Photo -> Caption).
    2. Records user DM activity in Valkey for 10-day / 30-day broadcast targeting.
    3. For normal chat messages (non-command), rolls 1/8 probability to show a Personalized Ad
       (max 2 times per ad per user) and distributes 3 Paise across user's Top 10 GCs!
    """
    msg = update.message
    if not msg or not msg.from_user or msg.from_user.is_bot:
        return

    user = msg.from_user
    uid = user.id
    now_ts = int(time.time())

    # Track DM activity in Valkey for last 10d/30d active user lookups
    if valkey_client:
        try:
            await valkey_client.zadd("promo:dm_active_users", {str(uid): now_ts})
        except Exception:
            pass

    if not pg_pool:
        return

    # 1. Check if user has a PAID Ad Campaign awaiting Link, Image, or Caption
    async with pg_pool.acquire() as conn:
        active_draft = await conn.fetchrow('''
            SELECT * FROM ad_campaigns
            WHERE advertiser_id = $1
              AND payment_status = 'PAID'
              AND submission_step IN ('AWAITING_LINK', 'AWAITING_IMAGE', 'AWAITING_CAPTION')
            ORDER BY ad_id DESC LIMIT 1;
        ''', uid)

        if active_draft:
            step = active_draft["submission_step"]
            ad_id = active_draft["ad_id"]

            # STEP A: AWAITING JOIN LINK / PRODUCT URL
            if step == "AWAITING_LINK":
                raw_link = (msg.text or "").strip()
                if not URL_REGEX.match(raw_link):
                    await msg.reply_text(
                        "<blockquote>❌ <b>Invalid Promotion Link!</b>\n\n"
                        "Please paste a valid Group/Channel Join Link or Website URL starting with <code>https://</code> or <code>t.me/</code>.</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    return

                if not raw_link.startswith("http"):
                    raw_link = "https://" + raw_link

                await conn.execute('''
                    UPDATE ad_campaigns SET target_url = $1, submission_step = 'AWAITING_IMAGE' WHERE ad_id = $2;
                ''', raw_link, ad_id)

                await msg.reply_text(
                    f"<blockquote>✅ <b>Step 1/3 Completed: Link Saved!</b>\n"
                    f"🔗 <code>{html.escape(raw_link)}</code>\n\n"
                    f"🖼️ <b>Step 2/3:</b> Now please send the <b>Promotional Image (Photo)</b> for your ad!</blockquote>",
                    parse_mode=ParseMode.HTML
                )
                return

            # STEP B: AWAITING PROMOTIONAL IMAGE
            if step == "AWAITING_IMAGE":
                if not msg.photo:
                    await msg.reply_text(
                        "<blockquote>📸 <b>Image Required!</b>\n\n"
                        "Please send a <b>Photo/Image</b> that you want to display in your promotional ad.</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    return

                photo_file_id = msg.photo[-1].file_id
                await conn.execute('''
                    UPDATE ad_campaigns SET photo_file_id = $1, submission_step = 'AWAITING_CAPTION' WHERE ad_id = $2;
                ''', photo_file_id, ad_id)

                await msg.reply_text(
                    "<blockquote>✅ <b>Step 2/3 Completed: Image Saved!</b>\n\n"
                    "✍️ <b>Step 3/3:</b> Now please send the <b>Ad Caption / Text</b> for your promotion!\n"
                    "<i>(Note: Do not put raw links in the caption; your link will be attached as a sleek Blue Button below the ad!)</i></blockquote>",
                    parse_mode=ParseMode.HTML
                )
                return

            # STEP C: AWAITING CAPTION -> FORWARD TO BOT OWNER FOR APPROVAL
            if step == "AWAITING_CAPTION":
                raw_caption = (msg.text or msg.caption or "").strip()
                if not raw_caption or len(raw_caption) < 5:
                    await msg.reply_text(
                        "<blockquote>✍️ <b>Caption Too Short!</b>\n\n"
                        "Please send a clear, engaging text caption (at least 5 characters) for your ad.</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    return

                # Strip any raw URLs from caption as per policy (button handles redirect)
                clean_caption = re.sub(r"https?://\S+|t\.me/\S+", "", raw_caption).strip()
                if not clean_caption:
                    clean_caption = "Check out our featured partner community and exclusive updates below!"

                await conn.execute('''
                    UPDATE ad_campaigns SET caption = $1, submission_step = 'AWAITING_APPROVAL' WHERE ad_id = $2;
                ''', clean_caption, ad_id)

                if valkey_client:
                    await valkey_client.delete(f"promo:setup_active:{uid}")

                # Forward to Bot Owner for Approval
                plan_info = AD_PLANS.get(active_draft["plan_key"], {})
                admin_caption = (
                    f"📢 <b>NEW AD CAMPAIGN FOR APPROVAL (#AD{ad_id})</b>\n\n"
                    f"<blockquote>👤 <b>Advertiser:</b> {html.escape(user.first_name or 'User')} (<code>{uid}</code>)\n"
                    f"📦 <b>Plan:</b> {plan_info.get('name', active_draft['plan_key'])}\n"
                    f"💳 <b>Payment Status:</b> <b>✅ PAID (₹{float(active_draft['amount']):.2f} | {active_draft['order_id']})</b>\n"
                    f"🔗 <b>Target URL:</b> <code>{html.escape(active_draft['target_url'])}</code></blockquote>\n"
                    f"<blockquote>📝 <b>Ad Caption Preview:</b>\n{html.escape(clean_caption)}</blockquote>"
                )
                admin_kb = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Approve & Launch Ad", callback_data=f"ad_approve_{ad_id}"),
                        InlineKeyboardButton("❌ Reject Ad", callback_data=f"ad_reject_{ad_id}")
                    ],
                    [InlineKeyboardButton("🔗 Test Destination Link", url=active_draft["target_url"])]
                ])

                try:
                    await context.bot.send_photo(
                        chat_id=ADMIN_OWNER_ID,
                        photo=active_draft["photo_file_id"],
                        caption=admin_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=admin_kb
                    )
                except Exception as e:
                    logger.error(f"Error forwarding ad {ad_id} to admin: {e}")

                await msg.reply_text(
                    f"🎉 <b>AD DETAILS SUBMITTED FOR APPROVAL!</b> ✨\n\n"
                    f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
                    f"💳 <b>Payment Status:</b> <code>✅ Verified &amp; Paid</code>\n"
                    f"⏳ <b>Status:</b> <b>Pending Admin Review</b></blockquote>\n"
                    f"<i>Your ad has been forwarded to the administrator. You will receive an instant notification as soon as it is approved!</i>",
                    parse_mode=ParseMode.HTML
                )
                return

    # 2. Not in Ad Setup -> Trigger 1/8 Probability Personalized Ad on Normal Messages!
    text = msg.text or msg.caption or ""
    if text.startswith("/"):
        return

    await maybe_send_personalized_ad(uid, context.bot)


async def track_group_engagement_fast(update: Update, context: CallbackContext):
    """Tracks user engagement in GCs in Valkey (7-day rolling window) for 3-Paise Top-10 GC ratio calculation."""
    msg = update.message
    if not msg or not msg.from_user or msg.from_user.is_bot:
        return
    if msg.chat.type not in ("group", "supergroup"):
        return

    uid = msg.from_user.id
    chat_id = msg.chat_id
    day_str = datetime.now(IST).strftime("%Y-%m-%d")

    # Replies to bot or messages in GC get tracked
    points = 2 if (msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.id == context.bot.id) else 1

    if valkey_client:
        try:
            key = f"promo:ugc:{day_str}:{uid}"
            pipe = valkey_client.pipeline(transaction=False)
            pipe.hincrby(key, str(chat_id), points)
            pipe.expire(key, 8 * 86400)
            await pipe.execute()
        except Exception:
            pass


# =========================================================
# 🎯 1/8 PROBABILITY PERSONALIZED AD & 3-PAISE RATIO ENGINE
# =========================================================
async def get_user_top_10_gcs_7d(user_id: int) -> List[Tuple[int, int]]:
    """
    Finds the Top 10 GCs where user_id was active in the last 7 days,
    combining real-time Valkey hashes AND existing `tg:webhook_logs:YYYY-MM-DD` / `tg:ai_replies:YYYY-MM-DD`.
    Returns [(chat_id, score), ...] sorted descending (max 10).
    """
    if not valkey_client:
        return []

    gc_scores: Dict[int, int] = {}
    now_ist = datetime.now(IST)
    days = [(now_ist - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    try:
        pipe = valkey_client.pipeline(transaction=False)
        for d in days:
            pipe.hgetall(f"promo:ugc:{d}:{user_id}")
        res = await pipe.execute()
        for hmap in res:
            if hmap:
                for cid_s, cnt_s in hmap.items():
                    cid = int(cid_s)
                    gc_scores[cid] = gc_scores.get(cid, 0) + int(cnt_s)
    except Exception:
        pass

    # Fallback: If empty, check existing Valkey `tg:ai_replies` for today & yesterday
    if not gc_scores:
        try:
            for d in days[:3]:
                items = await valkey_client.lrange(f"tg:ai_replies:{d}", -500, -1)
                for raw in items:
                    ev = json.loads(raw)
                    if ev.get("c") == "gc" and ev.get("uid") == user_id and ev.get("gid"):
                        cid = int(ev["gid"])
                        gc_scores[cid] = gc_scores.get(cid, 0) + 1
        except Exception:
            pass

    sorted_gcs = sorted(gc_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    return sorted_gcs


async def resolve_gc_owner_id(chat_id: int, bot) -> Optional[int]:
    """Resolves GC Owner ID from RAM, Valkey, or Telegram API."""
    if chat_id in gc_owner_ram_cache:
        return gc_owner_ram_cache[chat_id]

    if valkey_client:
        try:
            cached = await valkey_client.hget("promo:gc_owners", str(chat_id))
            if cached:
                oid = int(cached)
                gc_owner_ram_cache[chat_id] = oid
                return oid
        except Exception:
            pass

    if pg_pool:
        try:
            async with pg_pool.acquire() as conn:
                oid = await conn.fetchval("SELECT owner_id FROM gc_reward_claims WHERE chat_id = $1;", chat_id)
                if oid:
                    gc_owner_ram_cache[chat_id] = int(oid)
                    return int(oid)
        except Exception:
            pass

    oid, _, _ = await get_gc_creator_and_sync(chat_id, bot)
    return oid


async def record_ad_view_in_ram(ad_id: int, user_id: int):
    async with ram_metrics_lock:
        if ad_id not in ram_ad_metrics:
            ram_ad_metrics[ad_id] = {"clicks": 0, "views": 0, "engaged_users": set()}
        ram_ad_metrics[ad_id]["views"] += 1
        ram_ad_metrics[ad_id]["engaged_users"].add(user_id)


async def record_ad_click_in_ram(ad_id: int, user_id: int = 0):
    async with ram_metrics_lock:
        if ad_id not in ram_ad_metrics:
            ram_ad_metrics[ad_id] = {"clicks": 0, "views": 0, "engaged_users": set()}
        ram_ad_metrics[ad_id]["clicks"] += 1
        if user_id:
            ram_ad_metrics[ad_id]["engaged_users"].add(user_id)


async def maybe_send_personalized_ad(user_id: int, bot):
    """
    1/8 probability on normal user messages.
    Finds eligible active ads that have been shown < 2 times to this user.
    Forwards/copies the ad to user's DM, saves message_id in Valkey for 24h auto-delete,
    and splits 3 Paise (₹0.03) across user's Top 10 GCs in Server RAM!
    """
    if random.randint(1, 8) != 1:
        return

    if not active_ads_ram or not valkey_client:
        return

    now_ts = time.time()
    candidates = []
    weights = []

    for ad_id, ad in list(active_ads_ram.items()):
        if ad["expires_at"] <= now_ts:
            continue
        imp_key = f"promo:pers_imp:{ad_id}:{user_id}"
        imp_cnt = int(await valkey_client.get(imp_key) or 0)
        if imp_cnt < 2:
            candidates.append(ad)
            weights.append(int(ad.get("pers_weight", 1)))

    if not candidates:
        return

    chosen_ad = random.choices(candidates, weights=weights, k=1)[0]
    ad_id = chosen_ad["ad_id"]

    # Send using saved message_id (or photo fallback) with Blue Inline Button
    tracking_url = f"{PROMO_SERVER_URL}/r/{ad_id}?u={user_id}"
    ad_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=tracking_url)]
    ])

    try:
        sent_msg = await bot.copy_message(
            chat_id=user_id,
            from_chat_id=chosen_ad["saved_chat_id"],
            message_id=chosen_ad["saved_msg_id"],
            reply_markup=ad_button
        )
        sent_mid = sent_msg.message_id
    except Exception:
        try:
            formatted_caption = (
                f"📢 <b>Sponsored Promotion</b> ✨\n\n"
                f"<blockquote>{html.escape(chosen_ad.get('caption') or '')}</blockquote>\n"
                f"👇 <i>Tap the button below to explore:</i>"
            )
            s_photo = await bot.send_photo(
                chat_id=user_id,
                photo=chosen_ad["photo_file_id"],
                caption=formatted_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=ad_button
            )
            sent_mid = s_photo.message_id
        except Exception:
            return

    # 1. Increment impression count for this user (max 2) & save sent msg for 24h auto-delete
    imp_key = f"promo:pers_imp:{ad_id}:{user_id}"
    pipe = valkey_client.pipeline(transaction=False)
    pipe.incr(imp_key)
    pipe.expire(imp_key, 10 * 86400)
    pipe.rpush(f"promo:sent_msgs:{ad_id}", f"{user_id}:{sent_mid}")
    pipe.incr(f"promo:pers_reach:{ad_id}")
    await pipe.execute()

    await record_ad_view_in_ram(ad_id, user_id)

    # 2. Split 3 Paise (₹0.03) across User's Top 10 GCs in Server RAM
    top_gcs = await get_user_top_10_gcs_7d(user_id)
    if not top_gcs:
        return

    total_eng = sum(score for _, score in top_gcs)
    if total_eng <= 0:
        return

    async with ram_pers_lock:
        for cid, score in top_gcs:
            share_inr = 0.03 * (score / total_eng)
            owner_id = await resolve_gc_owner_id(cid, bot)
            if owner_id:
                ram_pers_owner_earnings[owner_id] = ram_pers_owner_earnings.get(owner_id, 0.0) + share_inr


# =========================================================
# 📡 BROADCAST ENGINE (100-BATCHES, 4X DAILY IST, 12H GAP)
# =========================================================
async def get_active_dm_users_days(days: int = 10) -> List[int]:
    """Fetches distinct users with DM activity in the last `days` days from Valkey."""
    users_set: Set[int] = set()
    now_ts = int(time.time())
    cutoff_ts = now_ts - (days * 86400)

    if valkey_client:
        try:
            z_members = await valkey_client.zrangebyscore("promo:dm_active_users", cutoff_ts, "+inf")
            for u_s in z_members:
                if str(u_s).isdigit():
                    users_set.add(int(u_s))
        except Exception:
            pass

        # Also read existing bot's `tg:webhook_logs:YYYY-MM-DD` for last `days` days
        now_ist = datetime.now(IST)
        for i in range(days):
            d_str = (now_ist - timedelta(days=i)).strftime("%Y-%m-%d")
            try:
                logs = await valkey_client.lrange(f"tg:webhook_logs:{d_str}", 0, -1)
                for raw in logs:
                    ev = json.loads(raw)
                    if ev.get("c") == "dm" and ev.get("uid"):
                        users_set.add(int(ev["uid"]))
            except Exception:
                pass

    return list(users_set)


async def execute_scheduled_broadcast_slot(bot):
    """
    Picks the earliest approved & paid ad in the queue where:
    - completed_broadcasts < total_broadcasts
    - last_broadcast_at is NULL or <= NOW() - 12 hours (12-hour minimum gap!)
    Broadcasts in batches of 100 users, then splits 3 Paise per user to Top 10 GC Owners in RAM -> DB!
    """
    if not pg_pool:
        return

    now_utc = datetime.now(timezone.utc)
    gap_cutoff = now_utc - timedelta(hours=12)

    async with pg_pool.acquire() as conn:
        ad_row = await conn.fetchrow('''
            SELECT * FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND payment_status = 'PAID'
              AND completed_broadcasts < total_broadcasts
              AND expires_at > $1
              AND (last_broadcast_at IS NULL OR last_broadcast_at <= $2)
            ORDER BY approved_at ASC, ad_id ASC
            LIMIT 1;
        ''', now_utc, gap_cutoff)

        if not ad_row:
            return

        ad_id = ad_row["ad_id"]
        await conn.execute('''
            UPDATE ad_campaigns
            SET completed_broadcasts = completed_broadcasts + 1,
                last_broadcast_at = $1
            WHERE ad_id = $2;
        ''', now_utc, ad_id)

    target_users = await get_active_dm_users_days(days=10)
    if not target_users:
        return

    successful_users: List[int] = []

    # Send in batches of 100 users
    for i in range(0, len(target_users), BATCH_SIZE):
        batch = target_users[i:i + BATCH_SIZE]
        sent_pairs: List[str] = []

        async def _send_one(uid: int):
            tracking_url = f"{PROMO_SERVER_URL}/r/{ad_id}?u={uid}"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=tracking_url)]])
            try:
                copied = await bot.copy_message(
                    chat_id=uid,
                    from_chat_id=ad_row["saved_chat_id"],
                    message_id=ad_row["saved_msg_id"],
                    reply_markup=kb
                )
                return uid, copied.message_id
            except Exception:
                return uid, None

        results = await asyncio.gather(*[_send_one(u) for u in batch], return_exceptions=True)
        for res in results:
            if isinstance(res, tuple) and res[1] is not None:
                u_id, m_id = res
                successful_users.append(u_id)
                sent_pairs.append(f"{u_id}:{m_id}")
                await record_ad_view_in_ram(ad_id, u_id)

        if valkey_client and sent_pairs:
            pipe = valkey_client.pipeline(transaction=False)
            pipe.rpush(f"promo:sent_msgs:{ad_id}", *sent_pairs)
            pipe.incrby(f"promo:brd_reach:{ad_id}", len(sent_pairs))
            await pipe.execute()

        await asyncio.sleep(1.2)

    # =========================================================
    # 💰 3-PAISE TOP 10 GC OWNER DISTRIBUTION IN SERVER RAM -> DB
    # =========================================================
    owner_ram_payouts: Dict[int, float] = {}

    for u_id in successful_users:
        top_gcs = await get_user_top_10_gcs_7d(u_id)
        if not top_gcs:
            continue
        total_eng = sum(score for _, score in top_gcs)
        if total_eng <= 0:
            continue

        for cid, score in top_gcs:
            share_inr = 0.03 * (score / total_eng)
            owner_id = await resolve_gc_owner_id(cid, bot)
            if owner_id:
                owner_ram_payouts[owner_id] = owner_ram_payouts.get(owner_id, 0.0) + share_inr

    # Update DB one by one for each GC Owner & send DM notification
    now_utc = datetime.now(timezone.utc)
    unlock_at = now_utc + timedelta(days=3)

    for owner_id, earned_inr in owner_ram_payouts.items():
        if earned_inr <= 0:
            continue
        async with pg_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO owner_earning_logs (owner_id, amount, source, ref_id, created_at, available_at)
                VALUES ($1, $2, 'BROADCAST_REVENUE', $3, $4, $5);
            ''', owner_id, round(earned_inr, 4), ad_id, now_utc, unlock_at)

            new_bal = await conn.fetchval('''
                INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance, updated_at)
                VALUES ($1, $2, 0, NOW())
                ON CONFLICT (owner_id) DO UPDATE SET
                    total_balance = owner_wallets.total_balance + $2,
                    updated_at = NOW()
                RETURNING total_balance;
            ''', owner_id, round(earned_inr, 4))

        earned_paise = earned_inr * 100.0
        webapp_url = f"{PROMO_SERVER_URL}/rewards?uid={owner_id}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏦 View Balance & Withdraw", web_app=WebAppInfo(url=webapp_url))]])
        dm_text = (
            f"💰 <b>PROMOTIONAL BROADCAST EARNINGS CREDITED!</b> ✨\n\n"
            f"<blockquote>📢 <b>Campaign Reference:</b> <code>#AD{ad_id}</code>\n"
            f"💵 <b>Earned This Broadcast:</b> <code>+₹{earned_inr:.2f} INR</code> (<code>{earned_paise:.2f} Paise</code>)\n"
            f"🏦 <b>Total Partner Balance:</b> <code>₹{float(new_bal):.2f} INR</code></blockquote>\n"
            f"<i>Thank you for keeping your community active! Earnings unlock for UPI withdrawal 3 days after credit.</i>"
        )
        try:
            await bot.send_message(chat_id=owner_id, text=dm_text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass


# =========================================================
# 🧹 5-MIN VALKEY FLUSH, 10 PM PAYOUT & 24H AUTO-DELETE
# =========================================================
async def flush_ram_metrics_and_pers_earnings_to_valkey():
    """Runs every 5 minutes: flushes click/view metrics & personalized owner earnings from RAM to Valkey."""
    if not valkey_client:
        return

    # 1. Flush Ad Clicks & Views
    async with ram_metrics_lock:
        snapshot_metrics = ram_ad_metrics.copy()
        ram_ad_metrics.clear()

    if snapshot_metrics:
        pipe = valkey_client.pipeline(transaction=False)
        for ad_id, m in snapshot_metrics.items():
            if m["clicks"] > 0:
                pipe.incrby(f"promo:clicks:{ad_id}", m["clicks"])
            if m["views"] > 0:
                pipe.incrby(f"promo:views:{ad_id}", m["views"])
            if m["engaged_users"]:
                pipe.sadd(f"promo:engaged:{ad_id}", *[str(u) for u in m["engaged_users"]])
        await pipe.execute()

    # 2. Flush Personalized Ad Owner Earnings from RAM to Valkey
    async with ram_pers_lock:
        snapshot_pers = ram_pers_owner_earnings.copy()
        ram_pers_owner_earnings.clear()

    if snapshot_pers:
        pipe = valkey_client.pipeline(transaction=False)
        for owner_id, amt in snapshot_pers.items():
            pipe.hincrbyfloat("promo:valkey_pers_owner_earnings", str(owner_id), round(amt, 6))
        await pipe.execute()


async def flush_10pm_personalized_earnings_to_db(bot):
    """Runs at 10:00 PM IST: moves accumulated Personalized Ad earnings from Valkey to PostgreSQL DB & notifies Owners."""
    if not valkey_client or not pg_pool:
        return

    await flush_ram_metrics_and_pers_earnings_to_valkey()
    all_earnings = await valkey_client.hgetall("promo:valkey_pers_owner_earnings")
    if not all_earnings:
        return

    await valkey_client.delete("promo:valkey_pers_owner_earnings")
    now_utc = datetime.now(timezone.utc)
    unlock_at = now_utc + timedelta(days=3)

    for owner_s, amt_s in all_earnings.items():
        owner_id = int(owner_s)
        earned_inr = float(amt_s)
        if earned_inr <= 0:
            continue

        async with pg_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO owner_earning_logs (owner_id, amount, source, created_at, available_at)
                VALUES ($1, $2, 'PERSONALIZED_AD_DAILY', $3, $4);
            ''', owner_id, round(earned_inr, 4), now_utc, unlock_at)

            new_bal = await conn.fetchval('''
                INSERT INTO owner_wallets (owner_id, total_balance, withdrawable_balance, updated_at)
                VALUES ($1, $2, 0, NOW())
                ON CONFLICT (owner_id) DO UPDATE SET
                    total_balance = owner_wallets.total_balance + $2,
                    updated_at = NOW()
                RETURNING total_balance;
            ''', owner_id, round(earned_inr, 4))

        earned_paise = earned_inr * 100.0
        webapp_url = f"{PROMO_SERVER_URL}/rewards?uid={owner_id}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏦 Open Partner Dashboard", web_app=WebAppInfo(url=webapp_url))]])
        dm_msg = (
            f"🌙 <b>DAILY PERSONALIZED AD REVENUE CREDITED! (10:00 PM IST)</b> ✨\n\n"
            f"<blockquote>🤖 <b>Revenue Source:</b> AI Chat Personalized Ads (Today)\n"
            f"💵 <b>Today's Earnings:</b> <code>+₹{earned_inr:.2f} INR</code> (<code>{earned_paise:.2f} Paise</code>)\n"
            f"🏦 <b>Updated Partner Balance:</b> <code>₹{float(new_bal):.2f} INR</code></blockquote>\n"
            f"<i>Your group members' daily activity continues to generate passive income for you!</i>"
        )
        try:
            await bot.send_message(chat_id=owner_id, text=dm_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass


async def cleanup_expired_ads_after_24h(bot):
    """
    Checks for campaigns where `expires_at + 24 hours <= NOW()` and `cleanup_done = FALSE`.
    Deletes all broadcast & personalized messages in batches of 100, and sends final stats to Advertiser!
    """
    if not pg_pool or not valkey_client:
        return

    await flush_ram_metrics_and_pers_earnings_to_valkey()
    cutoff_delete = datetime.now(timezone.utc) - timedelta(hours=24)

    async with pg_pool.acquire() as conn:
        expired_ads = await conn.fetch('''
            SELECT * FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND cleanup_done = FALSE
              AND expires_at <= $1;
        ''', cutoff_delete)

    for ad in expired_ads:
        ad_id = ad["ad_id"]
        advertiser_id = ad["advertiser_id"]

        sent_entries = await valkey_client.lrange(f"promo:sent_msgs:{ad_id}", 0, -1)

        # Delete in batches of 100
        for i in range(0, len(sent_entries), BATCH_SIZE):
            batch = sent_entries[i:i + BATCH_SIZE]
            del_tasks = []
            for item in batch:
                try:
                    c_str, m_str = str(item).split(":")
                    del_tasks.append(bot.delete_message(chat_id=int(c_str), message_id=int(m_str)))
                except Exception:
                    pass
            if del_tasks:
                await asyncio.gather(*del_tasks, return_exceptions=True)
            await asyncio.sleep(1.0)

        # Fetch final stats from Valkey
        total_clicks = int(await valkey_client.get(f"promo:clicks:{ad_id}") or 0)
        total_views = int(await valkey_client.get(f"promo:views:{ad_id}") or len(sent_entries))
        brd_reach = int(await valkey_client.get(f"promo:brd_reach:{ad_id}") or 0)
        pers_reach = int(await valkey_client.get(f"promo:pers_reach:{ad_id}") or 0)
        unique_engaged = int(await valkey_client.scard(f"promo:engaged:{ad_id}") or 0)

        async with pg_pool.acquire() as conn:
            await conn.execute("UPDATE ad_campaigns SET cleanup_done = TRUE WHERE ad_id = $1;", ad_id)

        await valkey_client.delete(
            f"promo:sent_msgs:{ad_id}",
            f"promo:clicks:{ad_id}",
            f"promo:views:{ad_id}",
            f"promo:brd_reach:{ad_id}",
            f"promo:pers_reach:{ad_id}",
            f"promo:engaged:{ad_id}"
        )

        report_msg = (
            f"📊 <b>CAMPAIGN #AD{ad_id} COMPLETED — FINAL ANALYTICS REPORT</b> 🏆\n\n"
            f"<blockquote>📦 <b>Package:</b> {AD_PLANS.get(ad['plan_key'], {}).get('name', 'Promo Plan')}\n"
            f"📢 <b>Broadcasts Delivered:</b> <code>{ad['completed_broadcasts']}/{ad['total_broadcasts']}</code>\n"
            f"👥 <b>Total Broadcast Users Reached:</b> <code>{brd_reach:,} Users</code>\n"
            f"🤖 <b>Personalized AI Feed Deliveries:</b> <code>{pers_reach:,} Impressions</code>\n"
            f"👁️ <b>Total Ad Views:</b> <code>{total_views:,} Views</code>\n"
            f"🎯 <b>Unique Users Engaged:</b> <code>{unique_engaged:,} Users</code>\n"
            f"🖱️ <b>Total Button Clicks:</b> <code>{total_clicks:,} Clicks</code></blockquote>\n"
            f"<blockquote>🧹 <b>Auto-Cleanup Completed:</b> All promotional messages have been automatically removed 24 hours after campaign completion. Use <code>/set_ad</code> anytime to launch your next campaign! ✨</blockquote>"
        )
        try:
            await bot.send_message(chat_id=advertiser_id, text=report_msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await refresh_active_ads_ram()


async def master_scheduler_worker(bot):
    """Master loop running every 60 seconds for 4x IST Broadcasts, 5m Valkey Flushes, 10 PM Payouts & 24h Deletions."""
    global last_executed_slot_key, last_10pm_flush_date
    await asyncio.sleep(10)
    last_5m_ts = time.time()

    while True:
        try:
            now_ist = datetime.now(IST)
            now_ts = time.time()

            # 1. Every 5 minutes: Flush RAM clicks/views & personalized earnings to Valkey
            if now_ts - last_5m_ts >= 300:
                last_5m_ts = now_ts
                await flush_ram_metrics_and_pers_earnings_to_valkey()
                await refresh_active_ads_ram()
                await cleanup_expired_ads_after_24h(bot)

            # 2. Check 4 Daily IST Broadcast Slots (7 AM, 12 PM, 6 PM, 9 PM)
            if now_ist.hour in BROADCAST_SLOTS_IST and now_ist.minute == 0:
                slot_key = f"{now_ist.strftime('%Y-%m-%d')}_{now_ist.hour}"
                if last_executed_slot_key != slot_key:
                    last_executed_slot_key = slot_key
                    asyncio.create_task(execute_scheduled_broadcast_slot(bot))

            # 3. Check 10:00 PM IST Daily Personalized Ad Earnings Flush to DB
            if now_ist.hour == 22 and now_ist.minute == 0:
                d_str = now_ist.strftime("%Y-%m-%d")
                if last_10pm_flush_date != d_str:
                    last_10pm_flush_date = d_str
                    asyncio.create_task(flush_10pm_personalized_earnings_to_db(bot))

        except Exception as e:
            logger.error(f"Master scheduler error: {e}")

        await asyncio.sleep(30)


# =========================================================
# 🌐 FASTAPI & PTB APPLICATION SETUP
# =========================================================
ptb_promo_app = Application.builder().token(BOT_TOKEN).build()

ptb_promo_app.add_handler(CommandHandler(["set_ad", "setad", "promote"], cmd_set_ad))
ptb_promo_app.add_handler(CommandHandler(["rewards", "earnings", "partner"], cmd_rewards))
ptb_promo_app.add_handler(CallbackQueryHandler(handle_promo_callbacks))
ptb_promo_app.add_handler(ChatMemberHandler(handle_gc_bot_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
ptb_promo_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (~filters.COMMAND), handle_private_input_or_ad))
ptb_promo_app.add_handler(MessageHandler(filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP, track_group_engagement_fast))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global valkey_client
    await init_promo_db()
    if VALKEY_URL:
        valkey_client = aioredis.from_url(VALKEY_URL, decode_responses=True)

    await ptb_promo_app.initialize()
    await ptb_promo_app.start()
    await refresh_active_ads_ram()

    rewards_webapp.init_webapp_dependencies(pg_pool, ptb_promo_app.bot, ADMIN_OWNER_ID)
    sched_task = asyncio.create_task(master_scheduler_worker(ptb_promo_app.bot))

    yield

    sched_task.cancel()
    await flush_ram_metrics_and_pers_earnings_to_valkey()
    await ptb_promo_app.stop()
    await ptb_promo_app.shutdown()
    if pg_pool:
        await pg_pool.close()


app = FastAPI(lifespan=lifespan)
app.include_router(rewards_webapp.webapp_router)


@app.post("/webhook")
async def receive_forwarded_webhook(request: Request):
    """Receives forwarded Telegram updates from the main bot server."""
    try:
        update_data = await request.json()
        update = Update.de_json(update_data, ptb_promo_app.bot)
        if update:
            asyncio.create_task(ptb_promo_app.process_update(update))
    except Exception as e:
        logger.error(f"Promo webhook error: {e}")
    return Response(content='{"status":"ok"}', status_code=200, media_type="application/json")


@app.post("/pay/varify")
@app.post("/cash")
async def cashfree_payment_webhook(request: Request):
    """
    Automatic Cashfree Payment Webhook handler (/pay/varify).
    Verifies payment status with Cashfree API, marks campaign as PAID,
    and automatically prompts the user in DM to send their Join Link!
    """
    try:
        payload = await request.json()
        order_data = payload.get("data", {}).get("order", {}) or payload.get("order", {})
        order_id = order_data.get("order_id") or payload.get("order_id")

        if not order_id:
            return JSONResponse({"status": "ignored", "reason": "no_order_id"})

        cf_mgr = CashfreePaymentManager(bot_username="bot")
        is_paid = await cf_mgr._check_cashfree_direct(order_id, CF_APP_ID, CF_SECRET_KEY)
        if not is_paid:
            return JSONResponse({"status": "pending"})

        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow('''
                UPDATE ad_campaigns
                SET payment_status = 'PAID',
                    submission_step = 'AWAITING_LINK'
                WHERE order_id = $1 AND payment_status != 'PAID'
                RETURNING ad_id, advertiser_id, amount, plan_key;
            ''', order_id)

        if ad_row:
            uid = ad_row["advertiser_id"]
            ad_id = ad_row["ad_id"]
            if valkey_client:
                # Set flag in Valkey so main bot knows user is entering Ad inputs in DM
                await valkey_client.setex(f"promo:setup_active:{uid}", 1800, "1")

            prompt_link_msg = (
                f"✅ <b>PAYMENT VERIFIED SUCCESSFULLY!</b> 🎉\n\n"
                f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
                f"🧾 <b>Order ID:</b> <code>{order_id}</code>\n"
                f"💰 <b>Amount Paid:</b> <code>₹{float(ad_row['amount']):.2f} INR</code></blockquote>\n"
                f"<blockquote>🔗 <b>Step 1/3 — Send Your Promotion Link:</b>\n"
                f"Now please <b>paste the Join Link or URL</b> of the Group, Channel, Bot, or Product you want to promote!</blockquote>"
            )
            try:
                await ptb_promo_app.bot.send_message(
                    chat_id=uid,
                    text=prompt_link_msg,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Could not send payment confirmation DM to {uid}: {e}")

        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Cashfree webhook error: {e}")
        return JSONResponse({"status": "error"}, status_code=500)


@app.get("/r/{ad_id}")
async def track_click_and_redirect(ad_id: int, u: Optional[int] = 0):
    """
    Blue Inline Button Click Callback & Redirector!
    Records click & user engagement in Server RAM (flushed every 5m to Valkey)
    and immediately redirects the user to the Advertiser's target URL.
    """
    await record_ad_click_in_ram(ad_id, u or 0)

    target_url = "https://t.me"
    if ad_id in active_ads_ram and active_ads_ram[ad_id].get("target_url"):
        target_url = active_ads_ram[ad_id]["target_url"]
    elif pg_pool:
        async with pg_pool.acquire() as conn:
            db_url = await conn.fetchval("SELECT target_url FROM ad_campaigns WHERE ad_id = $1;", ad_id)
            if db_url:
                target_url = db_url

    return RedirectResponse(url=target_url, status_code=302)


@app.get("/")
async def health_root():
    return {"service": "Tanya Promo & GC Partner Rewards Engine", "status": "online"}

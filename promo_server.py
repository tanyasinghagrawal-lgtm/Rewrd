import os
import re
import json
import time
import math
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
from fastapi.responses import JSONResponse
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    LabeledPrice
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    PreCheckoutQueryHandler,
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

PROMO_SERVER_URL = (
    os.getenv("PROMO_SERVER_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or "https://your-new-promo-service.onrender.com"
).rstrip("/")

CF_APP_ID = os.getenv("CF_APP_ID", "1156860244f6f72b79a1b882a860686511")
CF_SECRET_KEY = os.getenv("CF_SECRET_KEY", "cfsk_ma_prod_de549bfa768737e4a1e4a34b427b2b1b_e53c7d56")

ADMIN_OWNER_ID = int(os.getenv("ADMIN_OWNER_ID", "8431723612"))

BROADCAST_SLOTS_IST = [7, 12, 18, 21]
BATCH_SIZE = 100

# Group Reward & Add Links
GC_REWARDS_DEEP_LINK = "https://t.me/real_tanya_chat_bot?start=rewards"
GC_ADD_GROUP_LINK = "https://t.me/real_tanya_chat_bot?startgroup=true"

# Ad Plans Configuration (Note: max_pers_impressions for 100 & 200 is strictly internal/secret)
AD_PLANS: Dict[str, Dict[str, Any]] = {
    "plan_100": {
        "name": "Lite Personalized Plan",
        "price": 100,
        "broadcasts": 0,
        "days": 1,
        "pers_weight": 1,
        "max_pers_impressions": 1000,
        "pers_label": "Limited Personalized Ad Recommendations (24 Hours)"
    },
    "plan_200": {
        "name": "Starter Promo Plan",
        "price": 200,
        "broadcasts": 1,
        "days": 1,
        "pers_weight": 1,
        "max_pers_impressions": 1000,
        "pers_label": "Limited Personalized Ad Recommendations (1x)"
    },
    "plan_365": {
        "name": "Growth Promo Plan",
        "price": 365,
        "broadcasts": 2,
        "days": 2,
        "pers_weight": 3,
        "max_pers_impressions": 0,
        "pers_label": "3x Personalized Ad Recommendations (2 Days)"
    },
    "plan_500": {
        "name": "Pro Boost Plan",
        "price": 500,
        "broadcasts": 3,
        "days": 2,
        "pers_weight": 6,
        "max_pers_impressions": 0,
        "pers_label": "Max Personalized Ad Recommendations (2 Days)"
    },
    "plan_1000": {
        "name": "Ultra Viral Plan",
        "price": 1000,
        "broadcasts": 10,
        "days": 7,
        "pers_weight": 10,
        "max_pers_impressions": 0,
        "pers_label": "Max Personalized Ad Recommendations (7 Days)"
    }
}

# =========================================================
# ⚡ GLOBAL CONNECTIONS & SERVER RAM BUFFERS
# =========================================================
pg_pool: Optional[asyncpg.Pool] = None
valkey_client: Optional[aioredis.Redis] = None

# Active Ads Cache in RAM for O(1) 1/8 Personalized Ad Selection
active_ads_ram: Dict[int, Dict[str, Any]] = {}

# Server RAM Personalized Ad Owner Earnings Buffer (Flushed every 5 mins to Valkey -> 10 PM IST to DB)
ram_pers_owner_earnings: Dict[int, float] = {}
ram_pers_lock = asyncio.Lock()

# Group Owner ID Cache in Server RAM {chat_id: owner_id}
gc_owner_ram_cache: Dict[int, int] = {}

last_executed_slot_key: str = ""
last_10pm_flush_date: str = ""


def build_gc_partner_keyboard() -> InlineKeyboardMarkup:
    """Standard buttons for Group Chats: Check Balance via Bot Deep-Link + Add to Another GC."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Check Partner Balance & Rewards", url=GC_REWARDS_DEEP_LINK)],
        [InlineKeyboardButton("➕ Add This Bot to Another GC to Claim Real Money", url=GC_ADD_GROUP_LINK)]
    ])


def build_dm_partner_keyboard() -> InlineKeyboardMarkup:
    """Standard buttons for Private DM: Secure WebApp button + Add to Another GC."""
    webapp_url = f"{PROMO_SERVER_URL}/rewards"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏦 Open Partner Rewards WebApp", web_app=WebAppInfo(url=webapp_url))],
        [InlineKeyboardButton("➕ Add This Bot to Another GC to Claim Real Money", url=GC_ADD_GROUP_LINK)]
    ])


def ensure_ad_hashtag(caption_text: str) -> str:
    """Ensures #AD is always appended at the very end of the caption."""
    cleaned = (caption_text or "").strip()
    if not cleaned.endswith("#AD"):
        cleaned = f"{cleaned}\n\n#AD"
    return cleaned


def build_formatted_ad_caption(raw_caption: str) -> str:
    """Formats the ad caption in HTML and guarantees #AD at the very end."""
    base = re.sub(r"\s*#AD\s*$", "", (raw_caption or "").strip(), flags=re.IGNORECASE).strip()
    return (
        f"📢 <b>Sponsored Promotion</b> ✨\n\n"
        f"<blockquote>{html.escape(base)}</blockquote>\n\n"
        f"#AD"
    )


# =========================================================
# 💳 CASHFREE PAYMENT GATEWAY MANAGER
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
        self, transaction_id: int, amount: int,
        phone: str, cf_app_id: str, cf_secret: str
    ):
        order_id = f"txn_{transaction_id}"
        return_url = (
            f"https://t.me/{self.bot_username}"
            f"?start=success_txn_{transaction_id}"
        )
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
            "order_amount": int(amount),
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
                response = await client.get(url, headers=headers, timeout=10)
                data = response.json()
                if data.get("order_status") == "PAID":
                    return True
                return False
        except Exception as e:
            logger.error(f"CF Check Error: {e}")
            return False


# =========================================================
# 🗄️ DATABASE INITIALIZATION & MIGRATIONS
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

            CREATE TABLE IF NOT EXISTS owner_ad_spends (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                ad_id BIGINT NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS ad_campaigns (
                ad_id BIGSERIAL PRIMARY KEY,
                transaction_id BIGINT UNIQUE,
                order_id VARCHAR(64) UNIQUE,
                advertiser_id BIGINT NOT NULL,
                advertiser_name TEXT,
                plan_key VARCHAR(20) DEFAULT 'plan_200',
                amount NUMERIC(10, 2) DEFAULT 0,
                wallet_used NUMERIC(10, 2) DEFAULT 0,
                net_payable INT DEFAULT 0,
                payment_method VARCHAR(20) DEFAULT 'UPI',
                total_broadcasts INT DEFAULT 0,
                completed_broadcasts INT DEFAULT 0,
                duration_days INT DEFAULT 1,
                pers_weight INT DEFAULT 1,
                max_pers_impressions INT DEFAULT 0,
                payment_status VARCHAR(20) DEFAULT 'PENDING',
                submission_step VARCHAR(30) DEFAULT 'AWAITING_LINK',
                target_url TEXT DEFAULT NULL,
                photo_file_id TEXT DEFAULT NULL,
                caption TEXT DEFAULT NULL,
                saved_chat_id BIGINT DEFAULT NULL,
                saved_msg_id BIGINT DEFAULT NULL,
                approval_status VARCHAR(20) DEFAULT 'DRAFT',
                approved_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                last_broadcast_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                cleanup_done BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        ''')
        # Safe ALTER TABLE migrations for existing databases
        await conn.execute("ALTER TABLE ad_campaigns ADD COLUMN IF NOT EXISTS wallet_used NUMERIC(10, 2) DEFAULT 0;")
        await conn.execute("ALTER TABLE ad_campaigns ADD COLUMN IF NOT EXISTS net_payable INT DEFAULT 0;")
        await conn.execute("ALTER TABLE ad_campaigns ADD COLUMN IF NOT EXISTS payment_method VARCHAR(20) DEFAULT 'UPI';")
        await conn.execute("ALTER TABLE ad_campaigns ADD COLUMN IF NOT EXISTS max_pers_impressions INT DEFAULT 0;")

        # Preload known GC owners into Server RAM cache
        rows = await conn.fetch("SELECT chat_id, owner_id FROM gc_reward_claims WHERE clawed_back = FALSE;")
        for r in rows:
            gc_owner_ram_cache[int(r["chat_id"])] = int(r["owner_id"])

    logger.info("✅ Promo & GC Earnings PostgreSQL tables initialized & RAM Owner Cache loaded.")


async def refresh_active_ads_ram():
    """Loads currently running & approved ad campaigns into Server RAM."""
    if not pg_pool:
        return
    now_utc = datetime.now(timezone.utc)
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT ad_id, advertiser_id, saved_chat_id, saved_msg_id,
                   target_url, caption, photo_file_id, pers_weight,
                   max_pers_impressions, plan_key, expires_at
            FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND payment_status = 'PAID'
              AND expires_at > $1;
        ''', now_utc)

    new_map: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        ad_id = r["ad_id"]
        max_imp = int(r["max_pers_impressions"] or 0)
        if max_imp > 0 and valkey_client:
            cur_reach = int(await valkey_client.get(f"promo:pers_reach:{ad_id}") or 0)
            if cur_reach >= max_imp:
                # Reached secret 1,000 personalized impression limit
                if r["plan_key"] == "plan_100":
                    async with pg_pool.acquire() as conn:
                        await conn.execute("UPDATE ad_campaigns SET expires_at = NOW() WHERE ad_id = $1;", ad_id)
                continue

        new_map[ad_id] = {
            "ad_id": ad_id,
            "advertiser_id": r["advertiser_id"],
            "saved_chat_id": r["saved_chat_id"],
            "saved_msg_id": r["saved_msg_id"],
            "target_url": r["target_url"],
            "caption": r["caption"],
            "photo_file_id": r["photo_file_id"],
            "pers_weight": r["pers_weight"] or 1,
            "max_pers_impressions": max_imp,
            "plan_key": r["plan_key"],
            "expires_at": r["expires_at"].timestamp()
        }
    global active_ads_ram
    active_ads_ram = new_map


# =========================================================
# 🏆 GC OWNER ADD BONUS (₹10 / ₹25 — 7 DAYS LOCK) & CLAWBACK
# =========================================================
async def get_gc_creator_and_sync(chat_id: int, bot) -> Tuple[Optional[int], str, bool]:
    """Returns (creator_user_id, creator_first_name, is_bot_admin) and syncs Server RAM & Valkey."""
    try:
        admins = await bot.get_chat_administrators(chat_id)
        bot_member = next((m for m in admins if m.user.id == bot.id), None)
        is_bot_admin = bool(bot_member and bot_member.status in ("administrator", "creator"))

        creator = next((m for m in admins if m.status == "creator"), None)
        if not creator and admins:
            creator = next((m for m in admins if not m.user.is_bot), None)

        if creator and is_bot_admin:
            gc_owner_ram_cache[chat_id] = creator.user.id
            if valkey_client:
                await valkey_client.hset("promo:gc_owners", str(chat_id), str(creator.user.id))
            return creator.user.id, (creator.user.first_name or "Group Owner"), is_bot_admin
        elif not is_bot_admin:
            gc_owner_ram_cache.pop(chat_id, None)
            if valkey_client:
                await valkey_client.hdel("promo:gc_owners", str(chat_id))
        return (creator.user.id if creator else None), "Group Owner", is_bot_admin
    except Exception as e:
        logger.debug(f"Could not fetch admins for {chat_id}: {e}")
        return None, "Group Owner", False


async def revoke_gc_claim_if_early(chat_id: int, chat_title: str, bot):
    """Revokes the ₹25/₹10 GC addition bonus if the bot loses Admin or is kicked within 7 days."""
    if not pg_pool:
        return
    gc_owner_ram_cache.pop(chat_id, None)
    if valkey_client:
        await valkey_client.hdel("promo:gc_owners", str(chat_id))

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
                log_row = await conn.fetchrow('''
                    SELECT id FROM owner_earning_logs
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

                total_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)

            clawback_dm = (
                f"🚨 <b>PARTNER BONUS REVOKED (7-DAY REQUIREMENT NOT MET)</b>\n\n"
                f"<blockquote>🏛️ <b>Group:</b> <b>{html.escape(chat_title)}</b> (<code>{chat_id}</code>)\n"
                f"📉 <b>Amount Revoked:</b> <code>-₹{deduct_amt:.2f} INR</code>\n"
                f"💰 <b>Updated Balance:</b> <code>₹{float(total_bal):.2f} INR</code>\n"
                f"📌 <b>Reason:</b> The bot was removed or had its Admin privileges revoked before completing <b>7 continuous days</b> in the group.</blockquote>"
            )
            try:
                await bot.send_message(chat_id=owner_id, text=clawback_dm, parse_mode=ParseMode.HTML)
            except Exception:
                pass


async def handle_gc_bot_member_update(update: Update, context: CallbackContext):
    """
    Handles when bot is added/promoted to Admin in a GC (Credits ₹10 or ₹25 locked for 7 Days)
    AND when bot is removed/demoted within 7 days (Revokes claimed amount).
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

    # CASE 1: BOT ADDED & GIVEN ADMIN ACCESS
    if new_status in ("administrator", "creator") and old_status != "administrator":
        owner_id, owner_name, is_bot_admin = await get_gc_creator_and_sync(chat_id, context.bot)
        if not owner_id:
            owner_id = my_mem.from_user.id if my_mem.from_user else None
            owner_name = (my_mem.from_user.first_name or "Group Owner") if my_mem.from_user else "Group Owner"

        if not owner_id or not is_bot_admin:
            return

        gc_owner_ram_cache[chat_id] = owner_id
        if valkey_client:
            await valkey_client.hset("promo:gc_owners", str(chat_id), str(owner_id))

        safe_owner_name = html.escape(owner_name)
        owner_blue_mention = f'<b><a href="tg://user?id={owner_id}">{safe_owner_name}</a></b>'

        try:
            member_count = await context.bot.get_chat_member_count(chat_id)
        except Exception:
            member_count = 0

        reward_kb_gc = build_gc_partner_keyboard()
        reward_kb_dm = build_dm_partner_keyboard()

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
                    f"💵 <b>Amount Added:</b> <code>₹0.00 (Already Claimed)</code>\n"
                    f"📌 <b>Reason:</b> The joining bonus (<code>₹{prev_amt:.0f}</code>) for this group was already registered previously!</blockquote>\n"
                    f"💡 <i>You will continue to earn <b>80% Partner Revenue Share</b> from ads shown in DM to your active group members!</i>"
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
                    f"💵 <b>Amount Added:</b> <code>₹0.00</code>\n"
                    f"📌 <b>Reason:</b> A group requires <b>201+ members</b> (for ₹10) or <b>501+ members</b> (for ₹25) to qualify for the instant Admin Addition Bonus.</blockquote>\n"
                    f"✨ <i>Your group is now active for our <b>80% Ad Revenue Share</b> whenever your group members engage with the bot!</i>"
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

            # Credit GC Add Bonus with 7-Day Maturation Window
            now_utc = datetime.now(timezone.utc)
            unlock_at_7d = now_utc + timedelta(days=7)

            async with conn.transaction():
                await conn.execute('''
                    INSERT INTO gc_reward_claims (chat_id, chat_title, owner_id, owner_name, member_count, claimed_amount, claimed_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7);
                ''', chat_id, chat.title or "Group Chat", owner_id, owner_name, member_count, reward_inr, now_utc)

                await conn.execute('''
                    INSERT INTO owner_earning_logs (owner_id, amount, source, ref_id, created_at, available_at)
                    VALUES ($1, $2, 'GC_ADD_BONUS', $3, $4, $5);
                ''', owner_id, reward_inr, chat_id, now_utc, unlock_at_7d)

                new_tot, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)

        credit_msg = (
            f"🎉 <b>GC OWNER PARTNER REWARD REGISTERED!</b> 💸\n\n"
            f"<blockquote>👑 <b>Group Owner:</b> {owner_blue_mention}\n"
            f"🏛️ <b>Group:</b> <b>{chat_title}</b>\n"
            f"👥 <b>Verified Members:</b> <code>{member_count:,} Users</code>\n"
            f"💰 <b>Bonus Credited:</b> <code>+₹{reward_inr:.2f} INR</code>\n"
            f"🏦 <b>Total Partner Balance:</b> <code>₹{float(new_tot):.2f} INR</code></blockquote>\n"
            f"<blockquote>⏳ <b>7-Day Verification Rule:</b>\n"
            f"• This group addition bonus unlocks for withdrawal once the bot remains an active Admin in this group for <b>7 continuous days</b>.\n"
            f"• Removing the bot or revoking Admin access before 7 days will automatically cancel this bonus.</blockquote>"
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

    # CASE 2: BOT REMOVED OR ADMIN REVOKED
    if new_status in ("left", "kicked", "member") and old_status in ("administrator", "creator"):
        await revoke_gc_claim_if_early(chat_id, chat.title or "Group Chat", context.bot)


async def verify_pending_7d_gc_bonuses(bot):
    """
    Periodically checks all GC bonus claims within their 7-day window to ensure
    the bot has not been kicked or stripped of Admin privileges, and monitors GC activity logs.
    """
    if not pg_pool:
        return
    now_utc = datetime.now(timezone.utc)
    cutoff_7d = now_utc - timedelta(days=7)

    async with pg_pool.acquire() as conn:
        pending_claims = await conn.fetch('''
            SELECT chat_id, chat_title, owner_id
            FROM gc_reward_claims
            WHERE clawed_back = FALSE
              AND claimed_amount > 0
              AND claimed_at >= $1;
        ''', cutoff_7d)

    for row in pending_claims:
        cid = int(row["chat_id"])
        title = row["chat_title"] or "Group Chat"
        _, _, is_bot_admin = await get_gc_creator_and_sync(cid, bot)
        if not is_bot_admin:
            await revoke_gc_claim_if_early(cid, title, bot)


# =========================================================
# 📢 COMMANDS: /start, /rewards, /ad, /set_ad & MANAGEMENT
# =========================================================
async def send_rewards_hub_message(message, is_private: bool):
    text = (
        f"👑 <b>GC OWNER PARTNER REWARDS HUB</b> 💸\n\n"
        f"<blockquote>Earn real INR payouts by adding our bot as an Admin in your groups and receiving <b>80% of our promotional ad revenue</b>!</blockquote>\n"
        f"<blockquote>• <b>201–500 Members GC:</b> <code>+₹10.00</code> Bonus (Unlocks after 7 Days as Admin)\n"
        f"• <b>500+ Members GC:</b> <code>+₹25.00</code> Bonus (Unlocks after 7 Days as Admin)\n"
        f"• <b>80% Ad Revenue Share:</b> We only display ads in users' Private DMs (never in GCs) and share ₹0.30 per ad across their Top 10 active groups!\n"
        f"• <b>Fast UPI Withdrawals:</b> Normal ad earnings unlock in 3 Days (Min <code>₹100</code> — Max <code>₹500/day</code>)</blockquote>\n"
        f"👇 <i>Tap below to open your secure Partner Dashboard or add the bot to another group:</i>"
    )
    kb = build_dm_partner_keyboard() if is_private else build_gc_partner_keyboard()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def cmd_start_handler(update: Update, context: CallbackContext):
    """Handles /start deep links: start=rewards, start=reward, start=set_ad, start=success_txn_..."""
    msg = update.message
    if not msg or not msg.from_user:
        return

    args = context.args or []
    if args:
        arg0 = args[0].strip().lower()
        if arg0 in ("rewards", "reward"):
            await send_rewards_hub_message(msg, is_private=(msg.chat.type == "private"))
            return
        if arg0 in ("set_ad", "setad", "ad"):
            await cmd_set_ad(update, context)
            return
        if arg0.startswith("success_txn_"):
            await cmd_set_ad(update, context)
            return


async def cmd_rewards(update: Update, context: CallbackContext):
    """Opens the GC Owner Partner Rewards Hub."""
    msg = update.message
    if not msg or not msg.from_user:
        return
    await send_rewards_hub_message(msg, is_private=(msg.chat.type == "private"))


async def build_ad_management_dashboard(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Builds the /ad & /set_ad Campaign Management Dashboard showing balance and existing/pending/action-required ads."""
    now_utc = datetime.now(timezone.utc)
    async with pg_pool.acquire() as conn:
        total_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, uid)
        campaigns = await conn.fetch('''
            SELECT ad_id, plan_key, amount, net_payable, payment_status,
                   submission_step, approval_status, target_url,
                   completed_broadcasts, total_broadcasts, expires_at
            FROM ad_campaigns
            WHERE advertiser_id = $1
            ORDER BY ad_id DESC
            LIMIT 8;
        ''', uid)

    lines = [
        f"📢 <b>PROMOTIONAL AD CAMPAIGN MANAGER</b> 🚀\n",
        f"<blockquote>💰 <b>Your Available Earned Balance:</b> <code>₹{total_bal:.2f} INR</code>\n"
        f"<i>(Your earned balance is automatically applied when you purchase any ad plan!)</i></blockquote>\n"
    ]

    buttons = [[InlineKeyboardButton("➕ Create New Ad Campaign", callback_data="ad_create_new")]]

    if campaigns:
        lines.append("📋 <b>Your Recent Ad Campaigns:</b>\n")
        for c in campaigns:
            aid = c["ad_id"]
            p_name = AD_PLANS.get(c["plan_key"], {}).get("name", c["plan_key"])
            pay_st = c["payment_status"]
            app_st = c["approval_status"]
            step = c["submission_step"]
            url_str = html.escape(c["target_url"] or "Not set")

            if app_st == "ACTION_REQUIRED" or step == "ACTION_REQUIRED":
                lines.append(
                    f"<blockquote>🚨 <b>#AD{aid} — ACTION REQUIRED (Update Quickly!)</b>\n"
                    f"• <b>Plan:</b> {p_name} (✅ Paid)\n"
                    f"• <b>Notice:</b> Admin declined your previous ad content. Please resubmit your Link, Image, and Caption below!</blockquote>"
                )
                buttons.append([InlineKeyboardButton(f"🚨 Action Required: Update #AD{aid} Now", callback_data=f"ad_resubmit_{aid}")])

            elif pay_st == "PAID" and app_st == "PENDING":
                lines.append(
                    f"<blockquote>⏳ <b>#AD{aid} — ACTIVE IN ADMIN APPROVAL QUEUE</b>\n"
                    f"• <b>Plan:</b> {p_name} | <b>URL:</b> <code>{url_str}</code>\n"
                    f"• <b>Status:</b> Payment verified! Currently active in the Admin Review Queue.</blockquote>"
                )

            elif pay_st == "PAID" and app_st == "APPROVED" and c["expires_at"] and c["expires_at"] > now_utc:
                lines.append(
                    f"<blockquote>🟢 <b>#AD{aid} — ACTIVE &amp; RUNNING</b>\n"
                    f"• <b>Plan:</b> {p_name}\n"
                    f"• <b>Redirect URL:</b> <code>{url_str}</code></blockquote>"
                )

            elif pay_st == "PENDING":
                if step in ("AWAITING_LINK", "AWAITING_IMAGE", "AWAITING_CAPTION"):
                    lines.append(
                        f"<blockquote>📝 <b>#AD{aid} — INCOMPLETE DRAFT ({step.replace('AWAITING_', 'Need ')})</b>\n"
                        f"• Complete your ad details to proceed to plan selection &amp; payment.</blockquote>"
                    )
                    buttons.append([InlineKeyboardButton(f"✏️ Continue Filling Draft #AD{aid}", callback_data=f"ad_resume_draft_{aid}")])
                else:
                    lines.append(
                        f"<blockquote>💳 <b>#AD{aid} — SAVED (PAYMENT PENDING)</b>\n"
                        f"• <b>Redirect URL:</b> <code>{url_str}</code>\n"
                        f"• <b>Status:</b> Ad content saved! Select a plan &amp; complete payment to launch.</blockquote>"
                    )
                    buttons.append([InlineKeyboardButton(f"💳 Select Plan & Pay for #AD{aid}", callback_data=f"ad_select_plan_menu_{aid}")])
    else:
        lines.append(
            "<blockquote>✨ <b>How It Works:</b>\n"
            "1️⃣ Submit your Redirect Link, Promotional Image &amp; Caption.\n"
            "2️⃣ Select your Ad Plan (use your Earned Balance + pay any remaining amount via UPI or Telegram Stars ⭐).\n"
            "3️⃣ Once paid, your ad is sent to the Admin for quick approval!</blockquote>"
        )

    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def cmd_set_ad(update: Update, context: CallbackContext):
    """Handles /ad, /set_ad, /setad, /promote."""
    msg = update.message
    if not msg or not msg.from_user:
        return

    if msg.chat.type != "private":
        bot_uname = context.bot.username or "real_tanya_chat_bot"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Open Ad Campaign Manager in DM", url=f"https://t.me/{bot_uname}?start=set_ad")]
        ])
        await msg.reply_text(
            "<blockquote>📢 <b>Launch Your Promotional Campaign!</b>\n\n"
            "Please click the button below to manage or create your Ad Campaign in Private DM. ✨</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
        return

    text, kb = await build_ad_management_dashboard(msg.from_user.id)
    await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def show_plan_selection_for_ad(chat_id: int, user_id: int, ad_id: int, bot, edit_message=None):
    """Displays the 5 Promotional Ad Plans along with the user's Earned Balance and integer payable amounts."""
    async with pg_pool.acquire() as conn:
        total_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, user_id)

    plan_text = (
        f"📦 <b>SELECT PLAN FOR CAMPAIGN #AD{ad_id}</b> 🚀\n\n"
        f"<blockquote>💰 <b>Your Available Earned Balance:</b> <code>₹{total_bal:.2f} INR</code>\n"
        f"<i>Your earned balance will be automatically deducted! Any remaining amount can be paid in integer via <b>UPI</b> or <b>Telegram Stars (1 ₹ = 1 ⭐)</b>.</i></blockquote>\n\n"
        f"<blockquote>✨ <b>1. Lite Plan — ₹100 / 100 ⭐</b>\n"
        f"• <b>Broadcasts:</b> None\n"
        f"• <b>Smart Feed:</b> Limited Personalized Ad Recommendations in Users' DM\n\n"
        f"💎 <b>2. Starter Plan — ₹200 / 200 ⭐</b>\n"
        f"• <b>Broadcasts:</b> <code>1 Broadcast</code> (Last 30 Days Active Users)\n"
        f"• <b>Smart Feed:</b> Limited Personalized Ad Recommendations\n\n"
        f"🔥 <b>3. Growth Plan — ₹365 / 365 ⭐</b>\n"
        f"• <b>Broadcasts:</b> <code>2 Broadcasts in 2 Days</code>\n"
        f"• <b>Smart Feed:</b> <b>3x</b> Personalized Ad Recommendations (2 Days)\n\n"
        f"⚡ <b>4. Pro Boost Plan — ₹500 / 500 ⭐</b>\n"
        f"• <b>Broadcasts:</b> <code>3 Broadcasts in 2 Days</code>\n"
        f"• <b>Smart Feed:</b> <b>MAX</b> Personalized Ad Recommendations (2 Days)\n\n"
        f"👑 <b>5. Ultra Viral Plan — ₹1,000 / 1,000 ⭐</b>\n"
        f"• <b>Broadcasts:</b> <code>10 Broadcasts over 7 Days</code>\n"
        f"• <b>Smart Feed:</b> <b>MAX</b> Personalized Ad Recommendations (7 Days)</blockquote>\n"
        f"👇 <i>Choose your plan below to proceed to payment:</i>"
    )

    buttons = []
    for p_key, p_info in AD_PLANS.items():
        price_int = int(p_info["price"])
        rem_int = max(0, int(math.ceil(price_int - total_bal))) if total_bal < price_int else 0
        if total_bal > 0 and rem_int < price_int:
            btn_label = f"{p_info['name']} (₹{price_int} ➔ Pay ₹{rem_int} / {rem_int}⭐)"
        else:
            btn_label = f"{p_info['name']} — ₹{price_int} / {price_int} ⭐"
        buttons.append([InlineKeyboardButton(btn_label, callback_data=f"adselplan_{ad_id}_{p_key}")])

    buttons.append([InlineKeyboardButton("🔙 Back to Ad Manager", callback_data="ad_back_dashboard")])
    kb = InlineKeyboardMarkup(buttons)

    if edit_message:
        await edit_message.edit_text(plan_text, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await bot.send_message(chat_id=chat_id, text=plan_text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def submit_paid_ad_for_admin_approval(ad_id: int, bot):
    """Forwards a newly paid (or resubmitted) ad campaign to ADMIN_OWNER_ID for approval."""
    async with pg_pool.acquire() as conn:
        ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1;", ad_id)
        if not ad_row:
            return

    uid = ad_row["advertiser_id"]
    plan_info = AD_PLANS.get(ad_row["plan_key"], {})
    caption_with_tag = ensure_ad_hashtag(ad_row["caption"] or "")

    admin_caption = (
        f"📢 <b>AD CAMPAIGN FOR APPROVAL (#AD{ad_id})</b>\n\n"
        f"<blockquote>👤 <b>Advertiser:</b> {html.escape(ad_row['advertiser_name'] or 'User')} (<code>{uid}</code>)\n"
        f"📦 <b>Plan:</b> {plan_info.get('name', ad_row['plan_key'])}\n"
        f"💳 <b>Payment Status:</b> <b>✅ PAID (Plan ₹{float(ad_row['amount']):.0f} | Wallet: ₹{float(ad_row['wallet_used'] or 0):.2f} | Paid: {ad_row['net_payable']} via {ad_row['payment_method']})</b>\n"
        f"🔗 <b>Redirect URL:</b> <code>{html.escape(ad_row['target_url'] or '')}</code></blockquote>\n"
        f"<blockquote>📝 <b>Ad Caption Preview:</b>\n{html.escape(caption_with_tag)}</blockquote>"
    )
    admin_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve & Launch Ad", callback_data=f"ad_approve_{ad_id}"),
            InlineKeyboardButton("❌ Deny (Ask Resubmit)", callback_data=f"ad_reject_{ad_id}")
        ],
        [InlineKeyboardButton("🔗 Test Redirect Link", url=ad_row["target_url"])]
    ])

    try:
        await bot.send_photo(
            chat_id=ADMIN_OWNER_ID,
            photo=ad_row["photo_file_id"],
            caption=admin_caption,
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb
        )
    except Exception as e:
        logger.error(f"Error forwarding ad {ad_id} to admin: {e}")

    user_msg = (
        f"🎉 <b>AD CAMPAIGN #AD{ad_id} SENT FOR ADMIN APPROVAL!</b> ✨\n\n"
        f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
        f"📦 <b>Selected Plan:</b> <b>{plan_info.get('name', 'Promo Plan')}</b>\n"
        f"🔗 <b>Redirect URL:</b> <code>{html.escape(ad_row['target_url'] or '')}</code>\n"
        f"💳 <b>Payment Status:</b> <code>✅ Verified &amp; Paid</code>\n"
        f"⏳ <b>Status:</b> <b>Pending Admin Review</b></blockquote>\n"
        f"<i>Your campaign has been forwarded to the administrator and will go live as soon as it is approved!</i>"
    )
    try:
        await bot.send_message(chat_id=uid, text=user_msg, parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def finalize_ad_payment_and_notify(ad_id: int, payment_method: str, bot):
    """Marks an ad campaign as PAID, records any wallet deduction, and sends it to Admin Approval."""
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            ad_row = await conn.fetchrow('''
                SELECT * FROM ad_campaigns WHERE ad_id = $1 FOR UPDATE;
            ''', ad_id)
            if not ad_row or ad_row["payment_status"] == "PAID":
                return

            uid = ad_row["advertiser_id"]
            wallet_used = float(ad_row["wallet_used"] or 0.0)

            if wallet_used > 0:
                await conn.execute('''
                    INSERT INTO owner_ad_spends (owner_id, ad_id, amount, created_at)
                    VALUES ($1, $2, $3, NOW());
                ''', uid, ad_id, round(wallet_used, 2))
                await rewards_webapp.compute_and_sync_owner_wallet(conn, uid)

            await conn.execute('''
                UPDATE ad_campaigns
                SET payment_status = 'PAID',
                    payment_method = $1,
                    submission_step = 'AWAITING_APPROVAL',
                    approval_status = 'PENDING'
                WHERE ad_id = $2;
            ''', payment_method, ad_id)

    await submit_paid_ad_for_admin_approval(ad_id, bot)


async def estimate_broadcast_schedule(ad_id: int) -> str:
    """Calculates when this ad's first broadcast will go live based on earlier paid ads in queue."""
    if not pg_pool:
        return "Next Available Slot Today (IST)"

    async with pg_pool.acquire() as conn:
        ahead_count = await conn.fetchval('''
            SELECT COUNT(*) FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND payment_status = 'PAID'
              AND total_broadcasts > 0
              AND completed_broadcasts < total_broadcasts
              AND ad_id < $1;
        ''', ad_id) or 0

    now_ist = datetime.now(IST)
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


# =========================================================
# 🔘 CALLBACK QUERY HANDLER (AD FLOW, PAYMENTS & ADMIN)
# =========================================================
async def handle_promo_callbacks(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query or not query.from_user:
        return

    data = query.data or ""
    user = query.from_user
    uid = user.id

    # 1. Back to Ad Management Dashboard
    if data == "ad_back_dashboard":
        await query.answer()
        text, kb = await build_ad_management_dashboard(uid)
        await query.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 2. Start Creating a New Ad Campaign (Step 1: Fill Link -> Image -> Caption)
    if data == "ad_create_new":
        await query.answer()
        async with pg_pool.acquire() as conn:
            # Clear any unfinished unpaid step so only this new draft is active
            await conn.execute('''
                UPDATE ad_campaigns
                SET submission_step = 'DRAFT_PAUSED'
                WHERE advertiser_id = $1
                  AND payment_status = 'PENDING'
                  AND submission_step IN ('AWAITING_LINK', 'AWAITING_IMAGE', 'AWAITING_CAPTION');
            ''', uid)

            ad_id = await conn.fetchval('''
                INSERT INTO ad_campaigns (
                    advertiser_id, advertiser_name, payment_status,
                    submission_step, approval_status
                )
                VALUES ($1, $2, 'PENDING', 'AWAITING_LINK', 'DRAFT')
                RETURNING ad_id;
            ''', uid, user.first_name or "Advertiser")

        if valkey_client:
            await valkey_client.setex(f"promo:setup_active:{uid}", 1800, str(ad_id))

        await query.message.edit_text(
            f"🆕 <b>CREATE AD CAMPAIGN #AD{ad_id} — STEP 1/3</b>\n\n"
            f"<blockquote>🔗 <b>Send Your Promotion Redirect URL:</b>\n"
            f"Please paste the Group, Channel, Bot, or Website link where users should be redirected when they click your ad button (starting with <code>https://</code> or <code>t.me/</code>).</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    # 3. Resume an Unfinished Draft
    if data.startswith("ad_resume_draft_"):
        ad_id = int(data.replace("ad_resume_draft_", ""))
        await query.answer()
        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1 AND advertiser_id = $2;", ad_id, uid)
            if not ad_row:
                return
            await conn.execute('''
                UPDATE ad_campaigns
                SET submission_step = 'DRAFT_PAUSED'
                WHERE advertiser_id = $1 AND ad_id != $2
                  AND payment_status = 'PENDING'
                  AND submission_step IN ('AWAITING_LINK', 'AWAITING_IMAGE', 'AWAITING_CAPTION');
            ''', uid, ad_id)

            step = ad_row["submission_step"]
            if step not in ("AWAITING_LINK", "AWAITING_IMAGE", "AWAITING_CAPTION"):
                step = "AWAITING_LINK" if not ad_row["target_url"] else ("AWAITING_IMAGE" if not ad_row["photo_file_id"] else "AWAITING_CAPTION")
                await conn.execute("UPDATE ad_campaigns SET submission_step = $1 WHERE ad_id = $2;", step, ad_id)

        if valkey_client:
            await valkey_client.setex(f"promo:setup_active:{uid}", 1800, str(ad_id))

        prompts = {
            "AWAITING_LINK": f"🔗 <b>Resume #AD{ad_id} (Step 1/3):</b> Please send your Promotion Redirect URL (starting with <code>https://</code> or <code>t.me/</code>).",
            "AWAITING_IMAGE": f"🖼️ <b>Resume #AD{ad_id} (Step 2/3):</b> Please send the <b>Promotional Photo/Image</b> for your ad.",
            "AWAITING_CAPTION": f"✍️ <b>Resume #AD{ad_id} (Step 3/3):</b> Please send the <b>Ad Caption / Text</b> for your promotion."
        }
        await query.message.edit_text(f"<blockquote>{prompts[step]}</blockquote>", parse_mode=ParseMode.HTML)
        return

    # 4. Resubmit an Admin-Rejected Ad (Action Required)
    if data.startswith("ad_resubmit_"):
        ad_id = int(data.replace("ad_resubmit_", ""))
        await query.answer()
        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1 AND advertiser_id = $2;", ad_id, uid)
            if not ad_row:
                return
            await conn.execute('''
                UPDATE ad_campaigns
                SET submission_step = 'AWAITING_LINK'
                WHERE ad_id = $1;
            ''', ad_id)

        if valkey_client:
            await valkey_client.setex(f"promo:setup_active:{uid}", 1800, str(ad_id))

        await query.message.edit_text(
            f"✏️ <b>UPDATE &amp; RESUBMIT CAMPAIGN #AD{ad_id} — STEP 1/3</b>\n\n"
            f"<blockquote>🔗 <b>Send Updated Promotion Redirect URL:</b>\n"
            f"Please send your updated Group, Channel, Bot, or Website URL (starting with <code>https://</code> or <code>t.me/</code>). No extra payment is required!</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    # 5. Open Plan Selection Menu for a Saved Ad
    if data.startswith("ad_select_plan_menu_"):
        ad_id = int(data.replace("ad_select_plan_menu_", ""))
        await query.answer()
        await show_plan_selection_for_ad(query.message.chat_id, uid, ad_id, context.bot, edit_message=query.message)
        return

    # 6. Plan Selected -> Calculate Earned Balance Deduction & Show Payment Method Options (UPI vs Stars vs Full Balance)
    if data.startswith("adselplan_"):
        parts = data.split("_", 2)
        ad_id = int(parts[1])
        plan_key = parts[2]
        plan = AD_PLANS.get(plan_key)
        if not plan:
            await query.answer("Invalid plan.", show_alert=True)
            return

        await query.answer()
        price_int = int(plan["price"])

        async with pg_pool.acquire() as conn:
            total_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, uid)
            if total_bal >= price_int:
                wallet_used = float(price_int)
                net_payable = 0
            elif total_bal > 0:
                net_payable = max(1, int(math.ceil(price_int - total_bal)))
                wallet_used = round(price_int - net_payable, 2)
            else:
                wallet_used = 0.0
                net_payable = price_int

            await conn.execute('''
                UPDATE ad_campaigns
                SET plan_key = $1,
                    amount = $2,
                    wallet_used = $3,
                    net_payable = $4,
                    total_broadcasts = $5,
                    duration_days = $6,
                    pers_weight = $7,
                    max_pers_impressions = $8,
                    submission_step = 'AWAITING_PAYMENT'
                WHERE ad_id = $9 AND advertiser_id = $10;
            ''', plan_key, price_int, wallet_used, net_payable,
                plan["broadcasts"], plan["days"], plan["pers_weight"],
                plan["max_pers_impressions"], ad_id, uid)

        summary_msg = (
            f"💳 <b>CHECKOUT FOR CAMPAIGN #AD{ad_id}</b>\n\n"
            f"<blockquote>📦 <b>Selected Plan:</b> <b>{plan['name']}</b>\n"
            f"🏷️ <b>Plan Price:</b> <code>₹{price_int} INR</code> (or <code>{price_int} ⭐ Stars</code>)\n"
            f"💰 <b>Your Earned Balance:</b> <code>₹{total_bal:.2f} INR</code>\n"
            f"➖ <b>Earned Balance Applied:</b> <code>-₹{wallet_used:.2f} INR</code>\n"
            f"✅ <b>Net Payable Amount:</b> <code>₹{net_payable} INR</code> / <code>{net_payable} ⭐ Stars</code></blockquote>\n"
            f"👇 <i>Choose your preferred payment method below:</i>"
        )

        if net_payable <= 0:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✅ Pay Full ₹{price_int} Using Earned Balance", callback_data=f"adpay_wallet_{ad_id}")],
                [InlineKeyboardButton("🔙 Change Plan", callback_data=f"ad_select_plan_menu_{ad_id}")]
            ])
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🇮🇳 Pay ₹{net_payable} with UPI", callback_data=f"adpay_upi_{ad_id}")],
                [InlineKeyboardButton(f"⭐ Pay {net_payable} Telegram Stars", callback_data=f"adpay_stars_{ad_id}")],
                [InlineKeyboardButton("🔙 Change Plan", callback_data=f"ad_select_plan_menu_{ad_id}")]
            ])

        await query.message.edit_text(summary_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 7A. Pay Full Amount Using Earned Wallet Balance
    if data.startswith("adpay_wallet_"):
        ad_id = int(data.replace("adpay_wallet_", ""))
        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1 AND advertiser_id = $2;", ad_id, uid)
            if not ad_row or ad_row["payment_status"] == "PAID":
                await query.answer("Already processed!", show_alert=True)
                return
            total_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, uid)
            if total_bal < float(ad_row["amount"]):
                await query.answer("Insufficient earned balance. Please re-select the plan.", show_alert=True)
                return

        await query.answer("Payment completed via Earned Balance!")
        await query.message.edit_text(
            f"✅ <b>Paid ₹{float(ad_row['amount']):.0f} via Earned Balance for #AD{ad_id}!</b> Forwarding to Admin for approval...",
            parse_mode=ParseMode.HTML
        )
        await finalize_ad_payment_and_notify(ad_id, "WALLET", context.bot)
        return

    # 7B. Pay Remaining Integer Amount via UPI (Cashfree)
    if data.startswith("adpay_upi_"):
        ad_id = int(data.replace("adpay_upi_", ""))
        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1 AND advertiser_id = $2;", ad_id, uid)
            if not ad_row:
                await query.answer("Campaign not found.", show_alert=True)
                return

        net_payable = int(ad_row["net_payable"])
        plan = AD_PLANS.get(ad_row["plan_key"], {})
        await query.answer("Generating UPI payment link...")

        transaction_id = int(time.time() * 1000) + random.randint(10, 99)
        bot_uname = context.bot.username or "real_tanya_chat_bot"
        cf_mgr = CashfreePaymentManager(bot_username=bot_uname, user_id=uid)
        fake_phone = cf_mgr._get_fake_phone(uid)

        order_id, pay_link = await cf_mgr._create_cashfree_order(
            transaction_id=transaction_id,
            amount=net_payable,
            phone=fake_phone,
            cf_app_id=CF_APP_ID,
            cf_secret=CF_SECRET_KEY
        )

        if not order_id or not pay_link:
            await query.message.edit_text(
                "<blockquote>⚠️ <b>Payment Gateway Busy!</b>\n\n"
                "Could not generate the UPI link right now. Please try again in a few seconds.</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Retry", callback_data=f"adpay_upi_{ad_id}")]])
            )
            return

        async with pg_pool.acquire() as conn:
            await conn.execute('''
                UPDATE ad_campaigns
                SET transaction_id = $1, order_id = $2, payment_method = 'UPI'
                WHERE ad_id = $3;
            ''', transaction_id, order_id, ad_id)

        upi_msg = (
            f"🇮🇳 <b>COMPLETE UPI PAYMENT FOR #AD{ad_id}</b>\n\n"
            f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
            f"🧾 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"📦 <b>Plan:</b> <b>{plan.get('name', 'Promo Plan')}</b>\n"
            f"💰 <b>Earned Balance Used:</b> <code>₹{float(ad_row['wallet_used'] or 0):.2f} INR</code>\n"
            f"💳 <b>Net Payable via UPI:</b> <code>₹{net_payable} INR</code></blockquote>\n"
            f"<blockquote>💡 <b>Having Trouble Paying Inside Telegram?</b>\n"
            f"If you face any problem completing the payment inside the in-app browser, please <b>copy the URL below and open it directly in your phone's browser</b> (Chrome / Safari):\n\n"
            f"<code>{html.escape(pay_link)}</code></blockquote>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"💳 Pay ₹{net_payable} via UPI Now", url=pay_link)],
            [InlineKeyboardButton(f"⭐ Switch to Telegram Stars ({net_payable} ⭐)", callback_data=f"adpay_stars_{ad_id}")],
            [InlineKeyboardButton("🔙 Change Plan", callback_data=f"ad_select_plan_menu_{ad_id}")]
        ])
        await query.message.edit_text(upi_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 7C. Pay Remaining Integer Amount via Telegram Stars (1 ₹ = 1 Star)
    if data.startswith("adpay_stars_"):
        ad_id = int(data.replace("adpay_stars_", ""))
        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1 AND advertiser_id = $2;", ad_id, uid)
            if not ad_row:
                await query.answer("Campaign not found.", show_alert=True)
                return

        net_stars = int(ad_row["net_payable"])
        plan = AD_PLANS.get(ad_row["plan_key"], {})
        await query.answer("Generating Telegram Stars payment link...")

        transaction_id = int(time.time() * 1000) + random.randint(10, 99)
        payload_str = f"stars_ad_{ad_id}_{transaction_id}"

        try:
            invoice_link = await context.bot.create_invoice_link(
                title=f"Ad Campaign #AD{ad_id} — {plan.get('name', 'Promo Plan')}",
                description=f"Promotional Ad Campaign #AD{ad_id} ({net_stars} Telegram Stars)",
                payload=payload_str,
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label=plan.get("name", "Ad Campaign"), amount=net_stars)]
            )
        except Exception as e:
            logger.error(f"Stars invoice creation error: {e}")
            await query.answer("Could not create Telegram Stars invoice.", show_alert=True)
            return

        async with pg_pool.acquire() as conn:
            await conn.execute('''
                UPDATE ad_campaigns
                SET transaction_id = $1, order_id = $2, payment_method = 'STARS'
                WHERE ad_id = $3;
            ''', transaction_id, payload_str, ad_id)

        stars_msg = (
            f"⭐ <b>PAY WITH TELEGRAM STARS FOR #AD{ad_id}</b>\n\n"
            f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
            f"📦 <b>Plan:</b> <b>{plan.get('name', 'Promo Plan')}</b>\n"
            f"💰 <b>Earned Balance Applied:</b> <code>₹{float(ad_row['wallet_used'] or 0):.2f} INR</code>\n"
            f"⭐ <b>Payable Stars:</b> <code>{net_stars} ⭐ Telegram Stars</code> (1 ₹ = 1 ⭐)</blockquote>\n"
            f"👇 <i>Tap the button below to complete your payment instantly with Telegram Stars:</i>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⭐ Pay {net_stars} Telegram Stars Now", url=invoice_link)],
            [InlineKeyboardButton(f"🇮🇳 Switch to UPI (₹{net_stars})", callback_data=f"adpay_upi_{ad_id}")],
            [InlineKeyboardButton("🔙 Change Plan", callback_data=f"ad_select_plan_menu_{ad_id}")]
        ])
        await query.message.edit_text(stars_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 8. Bot Owner Approves or Rejects Ad Campaign
    if data.startswith("ad_approve_") or data.startswith("ad_reject_"):
        if uid != ADMIN_OWNER_ID:
            await query.answer("Unauthorized! Only Bot Owner can review ads.", show_alert=True)
            return

        ad_id = int(data.split("_")[-1])
        is_approve = data.startswith("ad_approve_")

        async with pg_pool.acquire() as conn:
            ad_row = await conn.fetchrow("SELECT * FROM ad_campaigns WHERE ad_id = $1;", ad_id)
            if not ad_row:
                await query.answer("Ad campaign not found.", show_alert=True)
                return
            if ad_row["approval_status"] not in ("PENDING", "ACTION_REQUIRED"):
                await query.answer(f"Already {ad_row['approval_status']}!", show_alert=True)
                return

            advertiser_id = ad_row["advertiser_id"]
            if not is_approve:
                await conn.execute('''
                    UPDATE ad_campaigns
                    SET approval_status = 'ACTION_REQUIRED',
                        submission_step = 'ACTION_REQUIRED'
                    WHERE ad_id = $1;
                ''', ad_id)

                await query.edit_message_caption(
                    caption=(query.message.caption or "") + "\n\n❌ <b>STATUS: DENIED (ACTION REQUIRED SENT TO USER)</b>",
                    parse_mode=ParseMode.HTML
                )
                resubmit_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"✏️ Update & Resubmit #AD{ad_id} Now", callback_data=f"ad_resubmit_{ad_id}")]
                ])
                try:
                    await context.bot.send_message(
                        chat_id=advertiser_id,
                        text=(
                            f"⚠️ <b>ACTION REQUIRED: AD CAMPAIGN #AD{ad_id} DECLINED BY ADMIN</b>\n\n"
                            f"<blockquote>Your submitted promotional ad content (link, image, or caption) was not approved by the administrator.\n\n"
                            f"🔄 <b>Please resubmit a new Promotion Link, Image, and Caption quickly!</b> Your payment is safe and you do not need to pay again. You can also check this anytime via <code>/ad</code>.</blockquote>"
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=resubmit_kb
                    )
                except Exception:
                    pass
                return

            # APPROVED! Direct Target URL in button + #AD at the end of caption
            direct_url = ad_row["target_url"]
            ad_button = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=direct_url)]
            ])
            formatted_ad_caption = build_formatted_ad_caption(ad_row["caption"] or "")

            sent_ad_msg = await context.bot.send_photo(
                chat_id=advertiser_id,
                photo=ad_row["photo_file_id"],
                caption=formatted_ad_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=ad_button
            )

            now_utc = datetime.now(timezone.utc)
            expires_at = now_utc + timedelta(days=int(ad_row["duration_days"] or 1))

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

        if valkey_client:
            await valkey_client.rpush(f"promo:sent_msgs:{ad_id}", f"{advertiser_id}:{sent_ad_msg.message_id}")

        await refresh_active_ads_ram()
        plan_info = AD_PLANS.get(ad_row["plan_key"], {})

        if int(ad_row["total_broadcasts"] or 0) > 0:
            est_schedule = await estimate_broadcast_schedule(ad_id)
            schedule_block = (
                f"<blockquote>🕒 <b>Estimated First Broadcast Slot:</b>\n"
                f"<code>{est_schedule}</code></blockquote>"
            )
        else:
            est_schedule = "Personalized DM Feed Active Immediately"
            schedule_block = (
                f"<blockquote>🤖 <b>Personalized DM Feed:</b>\n"
                f"Your ad is now live and being delivered to active users in Private DMs!</blockquote>"
            )

        approval_dm = (
            f"✅ <b>AD CAMPAIGN #AD{ad_id} APPROVED &amp; LIVE!</b> 🎉\n\n"
            f"<blockquote>📦 <b>Plan:</b> {plan_info.get('name', 'Promo Plan')}\n"
            f"🔗 <b>Redirect URL:</b> <code>{html.escape(direct_url)}</code>\n"
            f"📢 <b>Broadcasts Included:</b> <code>{ad_row['total_broadcasts']}</code></blockquote>\n"
            f"{schedule_block}"
        )
        try:
            await context.bot.send_message(chat_id=advertiser_id, text=approval_dm, parse_mode=ParseMode.HTML)
        except Exception:
            pass

        await query.edit_message_caption(
            caption=(query.message.caption or "") + f"\n\n✅ <b>APPROVED &amp; LIVE:</b>\n<code>{est_schedule}</code>",
            parse_mode=ParseMode.HTML
        )
        await query.answer("Ad Approved & Launched!")
        return

    # 9. Bot Owner Approves or Denies GC Owner Withdrawal
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
                    await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)
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
                    await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)
                    status_tag = "❌ <b>STATUS: DENIED (REFUNDED TO WALLET)</b>"
                    dm_notify = (
                        f"⚠️ <b>WITHDRAWAL REQUEST #WD{wd_id} DECLINED</b>\n\n"
                        f"<blockquote>💸 <b>Amount:</b> <code>₹{amount:.2f} INR</code>\n"
                        f"🏧 <b>UPI ID:</b> <code>{html.escape(upi_id)}</code>\n"
                        f"🔄 <b>Refund Status:</b> <code>₹{amount:.2f}</code> has been restored to your Withdrawable Balance.</blockquote>\n"
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
# ⭐ TELEGRAM STARS PAYMENT HANDLERS
# =========================================================
async def handle_precheckout_query(update: Update, context: CallbackContext):
    query = update.pre_checkout_query
    if not query:
        return
    await query.answer(ok=True)


async def handle_successful_payment(update: Update, context: CallbackContext):
    msg = update.message
    if not msg or not msg.successful_payment:
        return

    payload = msg.successful_payment.invoice_payload or ""
    if payload.startswith("stars_ad_"):
        parts = payload.split("_")
        if len(parts) >= 3 and parts[2].isdigit():
            ad_id = int(parts[2])
            await finalize_ad_payment_and_notify(ad_id, "STARS", context.bot)


# =========================================================
# 💬 PRIVATE MESSAGE & COMMAND ACTIVITY HANDLER (AD INPUTS + 1/8 AD)
# =========================================================
URL_REGEX = re.compile(r"^(https?://[^\s]+|t\.me/[^\s]+)$", re.IGNORECASE)


async def handle_private_activity_and_ad_setup(update: Update, context: CallbackContext):
    """
    Runs on ALL Private DM messages (both normal messages AND commands, never button clicks):
    1. Records user DM activity timestamp in Valkey.
    2. If user is sending a non-command message while filling an Ad draft (Link -> Image -> Caption), processes the step:
       - If the ad was already PAID (resubmission after admin denial), immediately sends it to Admin Approval!
       - If the ad is unpaid, saves it and shows the Plan Selection menu with Earned Balance!
    3. For any other DM activity (commands OR normal messages), triggers the 1/8 probability Personalized Ad!
    """
    msg = update.message
    if not msg or not msg.from_user or msg.from_user.is_bot:
        return
    if msg.successful_payment:
        return

    user = msg.from_user
    uid = user.id
    now_ts = int(time.time())
    raw_text = (msg.text or msg.caption or "").strip()
    is_command = raw_text.startswith("/")

    # 1. Track DM activity in Valkey for 7d / 30d active user lookups
    if valkey_client:
        try:
            await valkey_client.zadd("promo:dm_active_users", {str(uid): now_ts})
        except Exception:
            pass

    if not pg_pool:
        return

    # 2. Check if user is filling Ad details (only when message is not a slash command)
    if not is_command:
        async with pg_pool.acquire() as conn:
            active_draft = await conn.fetchrow('''
                SELECT * FROM ad_campaigns
                WHERE advertiser_id = $1
                  AND submission_step IN ('AWAITING_LINK', 'AWAITING_IMAGE', 'AWAITING_CAPTION')
                ORDER BY ad_id DESC LIMIT 1;
            ''', uid)

            if active_draft:
                step = active_draft["submission_step"]
                ad_id = active_draft["ad_id"]

                # STEP 1: REDIRECT URL
                if step == "AWAITING_LINK":
                    if not URL_REGEX.match(raw_text):
                        await msg.reply_text(
                            "<blockquote>❌ <b>Invalid Promotion Link!</b>\n\n"
                            "Please paste a valid Group, Channel, Bot, or Website URL starting with <code>https://</code> or <code>t.me/</code>.</blockquote>",
                            parse_mode=ParseMode.HTML
                        )
                        return

                    clean_link = raw_text if raw_text.startswith("http") else f"https://{raw_text}"
                    await conn.execute('''
                        UPDATE ad_campaigns SET target_url = $1, submission_step = 'AWAITING_IMAGE' WHERE ad_id = $2;
                    ''', clean_link, ad_id)

                    await msg.reply_text(
                        f"<blockquote>✅ <b>Step 1/3 Completed: Redirect URL Saved!</b>\n"
                        f"🔗 <code>{html.escape(clean_link)}</code>\n\n"
                        f"🖼️ <b>Step 2/3:</b> Now please send the <b>Promotional Image (Photo)</b> for your ad!</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    return

                # STEP 2: PROMOTIONAL PHOTO
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
                        "<i>(Note: Your redirect link will be attached as a button below the ad, and <code>#AD</code> will be added at the end of your caption.)</i></blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    return

                # STEP 3: CAPTION -> IF ALREADY PAID (RESUBMIT) GO TO ADMIN; ELSE SHOW PLAN SELECTION
                if step == "AWAITING_CAPTION":
                    if not raw_text or len(raw_text) < 5:
                        await msg.reply_text(
                            "<blockquote>✍️ <b>Caption Too Short!</b>\n\n"
                            "Please send a clear, engaging text caption (at least 5 characters) for your ad.</blockquote>",
                            parse_mode=ParseMode.HTML
                        )
                        return

                    clean_caption = re.sub(r"https?://\S+|t\.me/\S+", "", raw_text).strip()
                    if not clean_caption:
                        clean_caption = "Check out our featured partner community and exclusive updates below!"
                    clean_caption = ensure_ad_hashtag(clean_caption)

                    if valkey_client:
                        await valkey_client.delete(f"promo:setup_active:{uid}")

                    if active_draft["payment_status"] == "PAID":
                        await conn.execute('''
                            UPDATE ad_campaigns
                            SET caption = $1,
                                submission_step = 'AWAITING_APPROVAL',
                                approval_status = 'PENDING'
                            WHERE ad_id = $2;
                        ''', clean_caption, ad_id)
                        await submit_paid_ad_for_admin_approval(ad_id, context.bot)
                        return
                    else:
                        await conn.execute('''
                            UPDATE ad_campaigns
                            SET caption = $1,
                                submission_step = 'AWAITING_PLAN',
                                approval_status = 'DRAFT'
                            WHERE ad_id = $2;
                        ''', clean_caption, ad_id)

                        await show_plan_selection_for_ad(msg.chat_id, uid, ad_id, context.bot)
                        return

    # 3. Trigger 1/8 Probability Personalized Ad on ANY DM Activity (Commands or Normal Messages)
    await maybe_send_personalized_ad(uid, context.bot)


async def track_group_engagement_fast(update: Update, context: CallbackContext):
    """
    Tracks both normal GC messages AND bot engagement in Valkey (7-day window)
    plus records 7d general activity timestamp for broadcast reporting.
    """
    msg = update.message
    if not msg or not msg.from_user or msg.from_user.is_bot:
        return
    if msg.chat.type not in ("group", "supergroup"):
        return

    uid = msg.from_user.id
    chat_id = msg.chat_id
    now_ts = int(time.time())
    day_str = datetime.now(IST).strftime("%Y-%m-%d")

    is_bot_interaction = bool(
        (msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.id == context.bot.id)
        or ((msg.text or "").strip().startswith("/"))
    )

    if valkey_client:
        try:
            msg_key = f"promo:ugc_msg:{day_str}:{uid}"
            bot_key = f"promo:ugc_bot:{day_str}:{uid}"
            pipe = valkey_client.pipeline(transaction=False)
            pipe.hincrby(msg_key, str(chat_id), 1)
            pipe.expire(msg_key, 8 * 86400)
            if is_bot_interaction:
                pipe.hincrby(bot_key, str(chat_id), 1)
                pipe.expire(bot_key, 8 * 86400)
            pipe.zadd("promo:any_active_users", {str(uid): now_ts})
            await pipe.execute()
        except Exception:
            pass


# =========================================================
# 🎯 50% BOT + 50% NORMAL MSG RATIO ENGINE (₹0.30 / AD)
# =========================================================
async def get_user_top_10_gcs_weighted_7d(user_id: int) -> List[Tuple[int, float]]:
    """
    Calculates the user's Top 10 GCs over the last 7 days using Valkey logs:
    - 50% weight from Bot Activity in GC (`promo:ugc_bot` + `tg:ai_replies:YYYY-MM-DD`)
    - 50% weight from Total/Normal Messages in GC (`promo:ugc_msg` + `tg:webhook_logs:YYYY-MM-DD`)
    Returns [(chat_id, normalized_share_0_to_1), ...] for up to Top 10 GCs.
    """
    if not valkey_client:
        return []

    bot_counts: Dict[int, float] = {}
    msg_counts: Dict[int, float] = {}

    now_ist = datetime.now(IST)
    days = [(now_ist - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    try:
        pipe = valkey_client.pipeline(transaction=False)
        for d in days:
            pipe.hgetall(f"promo:ugc_bot:{d}:{user_id}")
            pipe.hgetall(f"promo:ugc_msg:{d}:{user_id}")
            pipe.hgetall(f"promo:ugc:{d}:{user_id}")
        res = await pipe.execute()

        for idx in range(0, len(res), 3):
            b_map = res[idx] or {}
            m_map = res[idx + 1] or {}
            legacy_map = res[idx + 2] or {}

            for cid_s, val_s in b_map.items():
                cid = int(cid_s)
                bot_counts[cid] = bot_counts.get(cid, 0.0) + float(val_s)

            for cid_s, val_s in m_map.items():
                cid = int(cid_s)
                msg_counts[cid] = msg_counts.get(cid, 0.0) + float(val_s)

            for cid_s, val_s in legacy_map.items():
                cid = int(cid_s)
                if cid not in msg_counts:
                    msg_counts[cid] = msg_counts.get(cid, 0.0) + float(val_s)
    except Exception:
        pass

    # Also read Valkey `tg:webhook_logs:YYYY-MM-DD` (normal GC msgs) & `tg:ai_replies:YYYY-MM-DD` (bot activity)
    if not msg_counts and not bot_counts:
        try:
            for d in days:
                ai_items = await valkey_client.lrange(f"tg:ai_replies:{d}", -600, -1)
                for raw in ai_items:
                    ev = json.loads(raw)
                    if ev.get("c") == "gc" and ev.get("uid") == user_id and ev.get("gid"):
                        cid = int(ev["gid"])
                        bot_counts[cid] = bot_counts.get(cid, 0.0) + 1.0

                wh_items = await valkey_client.lrange(f"tg:webhook_logs:{d}", -1000, -1)
                for raw in wh_items:
                    ev = json.loads(raw)
                    if ev.get("c") == "gc" and ev.get("uid") == user_id and ev.get("gid"):
                        cid = int(ev["gid"])
                        msg_counts[cid] = msg_counts.get(cid, 0.0) + 1.0
        except Exception:
            pass

    all_cids = set(bot_counts.keys()) | set(msg_counts.keys())
    if not all_cids:
        return []

    total_bot = sum(bot_counts.values())
    total_msg = sum(msg_counts.values())

    combined_scores: Dict[int, float] = {}
    for cid in all_cids:
        b_ratio = (bot_counts.get(cid, 0.0) / total_bot) if total_bot > 0 else 0.0
        m_ratio = (msg_counts.get(cid, 0.0) / total_msg) if total_msg > 0 else 0.0

        if total_bot > 0 and total_msg > 0:
            score = (0.5 * b_ratio) + (0.5 * m_ratio)
        elif total_msg > 0:
            score = m_ratio
        else:
            score = b_ratio

        if score > 0:
            combined_scores[cid] = score

    top_10 = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    top_sum = sum(w for _, w in top_10)
    if top_sum <= 0:
        return []

    return [(cid, w / top_sum) for cid, w in top_10]


async def resolve_gc_owner_id(chat_id: int, bot) -> Optional[int]:
    """Resolves GC Owner ID rapidly from Server RAM, then Valkey, then DB/API."""
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
                oid = await conn.fetchval(
                    "SELECT owner_id FROM gc_reward_claims WHERE chat_id = $1 AND clawed_back = FALSE;",
                    chat_id
                )
                if oid:
                    gc_owner_ram_cache[chat_id] = int(oid)
                    if valkey_client:
                        await valkey_client.hset("promo:gc_owners", str(chat_id), str(oid))
                    return int(oid)
        except Exception:
            pass

    oid, _, is_admin = await get_gc_creator_and_sync(chat_id, bot)
    return oid if is_admin else None


async def maybe_send_personalized_ad(user_id: int, bot):
    """
    1/8 probability on ANY user DM activity (except button clicks).
    Respects the secret 1,000 impression cap on ₹100 and ₹200 plans.
    Uses direct redirect URL and distributes ₹0.30 (0.3 Rs) across Top 10 GCs (50% Bot + 50% Normal Msg ratio).
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

        max_imp = int(ad.get("max_pers_impressions") or 0)
        if max_imp > 0:
            cur_reach = int(await valkey_client.get(f"promo:pers_reach:{ad_id}") or 0)
            if cur_reach >= max_imp:
                active_ads_ram.pop(ad_id, None)
                if ad.get("plan_key") == "plan_100" and pg_pool:
                    async with pg_pool.acquire() as conn:
                        await conn.execute("UPDATE ad_campaigns SET expires_at = NOW() WHERE ad_id = $1;", ad_id)
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
    direct_url = chosen_ad["target_url"]

    ad_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=direct_url)]
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
            formatted_caption = build_formatted_ad_caption(chosen_ad.get("caption") or "")
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

    imp_key = f"promo:pers_imp:{ad_id}:{user_id}"
    pipe = valkey_client.pipeline(transaction=False)
    pipe.incr(imp_key)
    pipe.expire(imp_key, 10 * 86400)
    pipe.rpush(f"promo:sent_msgs:{ad_id}", f"{user_id}:{sent_mid}")
    pipe.incr(f"promo:pers_reach:{ad_id}")
    res = await pipe.execute()

    new_reach = int(res[-1] or 0)
    max_imp = int(chosen_ad.get("max_pers_impressions") or 0)
    if max_imp > 0 and new_reach >= max_imp:
        active_ads_ram.pop(ad_id, None)
        if chosen_ad.get("plan_key") == "plan_100" and pg_pool:
            async with pg_pool.acquire() as conn:
                await conn.execute("UPDATE ad_campaigns SET expires_at = NOW() WHERE ad_id = $1;", ad_id)

    # Distribute ₹0.30 (0.3 Rs) across User's Top 10 GCs (50% Bot + 50% Normal Msg ratio)
    top_gcs = await get_user_top_10_gcs_weighted_7d(user_id)
    if not top_gcs:
        return

    async with ram_pers_lock:
        for cid, norm_weight in top_gcs:
            share_inr = 0.30 * norm_weight
            owner_id = await resolve_gc_owner_id(cid, bot)
            if owner_id:
                ram_pers_owner_earnings[owner_id] = ram_pers_owner_earnings.get(owner_id, 0.0) + share_inr


# =========================================================
# 📡 BROADCAST ENGINE (LAST 30D USERS, ₹0.30 SHARE & ADMIN REPORT)
# =========================================================
async def get_active_users_sets() -> Tuple[List[int], Set[int], Set[int]]:
    """
    Returns:
    1. target_30d_dm_users: Users active in DM in the last 30 days
    2. active_7d_dm_set: Users active in Bot DM in the last 7 days
    3. active_7d_any_set: Users active in either DM or any GC in the last 7 days
    """
    users_30d_dm: Set[int] = set()
    active_7d_dm: Set[int] = set()
    active_7d_any: Set[int] = set()

    now_ts = int(time.time())
    cutoff_30d = now_ts - (30 * 86400)
    cutoff_7d = now_ts - (7 * 86400)

    if valkey_client:
        try:
            z_30d = await valkey_client.zrangebyscore("promo:dm_active_users", cutoff_30d, "+inf")
            for u_s in z_30d:
                if str(u_s).isdigit():
                    users_30d_dm.add(int(u_s))

            z_7d_dm = await valkey_client.zrangebyscore("promo:dm_active_users", cutoff_7d, "+inf")
            for u_s in z_7d_dm:
                if str(u_s).isdigit():
                    uid = int(u_s)
                    active_7d_dm.add(uid)
                    active_7d_any.add(uid)

            z_7d_any = await valkey_client.zrangebyscore("promo:any_active_users", cutoff_7d, "+inf")
            for u_s in z_7d_any:
                if str(u_s).isdigit():
                    active_7d_any.add(int(u_s))
        except Exception:
            pass

        now_ist = datetime.now(IST)
        for i in range(30):
            d_str = (now_ist - timedelta(days=i)).strftime("%Y-%m-%d")
            try:
                logs = await valkey_client.lrange(f"tg:webhook_logs:{d_str}", 0, -1)
                for raw in logs:
                    ev = json.loads(raw)
                    uid = ev.get("uid")
                    if not uid:
                        continue
                    uid_int = int(uid)
                    if ev.get("c") == "dm":
                        users_30d_dm.add(uid_int)
                        if i < 7:
                            active_7d_dm.add(uid_int)
                            active_7d_any.add(uid_int)
                    elif ev.get("c") == "gc" and i < 7:
                        active_7d_any.add(uid_int)
            except Exception:
                pass

    return list(users_30d_dm), active_7d_dm, active_7d_any


async def execute_scheduled_broadcast_slot(bot):
    """
    Picks the earliest approved & paid ad with remaining broadcasts (total_broadcasts > 0),
    broadcasts to last 30 days active DM users, distributes ₹0.30 per user across Top 10 GCs,
    and sends the 7-day DM vs DM/GC activity breakdown report to ADMIN_OWNER_ID!
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
              AND total_broadcasts > 0
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

    target_users, active_7d_dm_set, active_7d_any_set = await get_active_users_sets()
    if not target_users:
        return

    successful_users: List[int] = []
    direct_url = ad_row["target_url"]

    for i in range(0, len(target_users), BATCH_SIZE):
        batch = target_users[i:i + BATCH_SIZE]
        sent_pairs: List[str] = []

        async def _send_one(uid: int):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Join / Visit Sponsored Link ↗️", url=direct_url)]])
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

        if valkey_client and sent_pairs:
            pipe = valkey_client.pipeline(transaction=False)
            pipe.rpush(f"promo:sent_msgs:{ad_id}", *sent_pairs)
            pipe.incrby(f"promo:brd_reach:{ad_id}", len(sent_pairs))
            await pipe.execute()

        await asyncio.sleep(1.2)

    # Send Broadcast Delivery & 7-Day Activity Breakdown Report to Bot Owner (ADMIN_OWNER_ID)
    delivered_count = len(successful_users)
    delivered_7d_dm = sum(1 for u in successful_users if u in active_7d_dm_set)
    delivered_7d_any = sum(1 for u in successful_users if u in active_7d_any_set)

    owner_report = (
        f"📡 <b>BROADCAST COMPLETED REPORT — #AD{ad_id}</b> 📊\n\n"
        f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
        f"🔗 <b>Redirect URL:</b> <code>{html.escape(direct_url)}</code>\n"
        f"👥 <b>Total Broadcast Delivered (30d Active):</b> <code>{delivered_count:,} Users</code>\n"
        f"💬 <b>Active in Bot DM (Last 7 Days):</b> <code>{delivered_7d_dm:,} Users</code>\n"
        f"🌐 <b>Active in DM or Any GC (Last 7 Days):</b> <code>{delivered_7d_any:,} Users</code></blockquote>"
    )
    try:
        await bot.send_message(chat_id=ADMIN_OWNER_ID, text=owner_report, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Could not send broadcast report to admin: {e}")

    # Distribute ₹0.30 (0.3 Rs) per user across User's Top 10 GCs (50% Bot + 50% Normal Msg ratio)
    owner_ram_payouts: Dict[int, float] = {}

    for u_id in successful_users:
        top_gcs = await get_user_top_10_gcs_weighted_7d(u_id)
        if not top_gcs:
            continue
        for cid, norm_weight in top_gcs:
            share_inr = 0.30 * norm_weight
            owner_id = await resolve_gc_owner_id(cid, bot)
            if owner_id:
                owner_ram_payouts[owner_id] = owner_ram_payouts.get(owner_id, 0.0) + share_inr

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

            new_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)

        earned_paise = earned_inr * 100.0
        kb = build_dm_partner_keyboard()
        dm_text = (
            f"💰 <b>PROMOTIONAL BROADCAST EARNINGS CREDITED!</b> ✨\n\n"
            f"<blockquote>📢 <b>Campaign Reference:</b> <code>#AD{ad_id}</code>\n"
            f"💵 <b>Earned This Broadcast:</b> <code>+₹{earned_inr:.2f} INR</code> (<code>{earned_paise:.1f} Paise</code>)\n"
            f"🏦 <b>Total Partner Balance:</b> <code>₹{float(new_bal):.2f} INR</code></blockquote>\n"
            f"<i>Thank you for keeping your community active! Ad earnings unlock for UPI withdrawal 3 days after credit.</i>"
        )
        try:
            await bot.send_message(chat_id=owner_id, text=dm_text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass


# =========================================================
# 🧹 5-MIN VALKEY FLUSH, 10 PM PAYOUT & CAMPAIGN END NOTICE
# =========================================================
async def flush_ram_pers_earnings_to_valkey():
    """Flushes personalized ad owner earnings from Server RAM to Valkey every 5 minutes."""
    if not valkey_client:
        return

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

    await flush_ram_pers_earnings_to_valkey()
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

            new_bal, _, _, _ = await rewards_webapp.compute_and_sync_owner_wallet(conn, owner_id)

        earned_paise = earned_inr * 100.0
        kb = build_dm_partner_keyboard()
        dm_msg = (
            f"🌙 <b>DAILY PERSONALIZED AD REVENUE CREDITED! (10:00 PM IST)</b> ✨\n\n"
            f"<blockquote>🤖 <b>Revenue Source:</b> Personalized DM Ads (Today)\n"
            f"💵 <b>Today's Earnings:</b> <code>+₹{earned_inr:.2f} INR</code> (<code>{earned_paise:.1f} Paise</code>)\n"
            f"🏦 <b>Updated Partner Balance:</b> <code>₹{float(new_bal):.2f} INR</code></blockquote>\n"
            f"<i>Your group members' daily activity continues to generate passive income for you!</i>"
        )
        try:
            await bot.send_message(chat_id=owner_id, text=dm_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass


async def cleanup_expired_ads(bot):
    """
    Checks for expired campaigns, deletes sent promotional messages,
    and sends a simple Campaign Ended notification WITHOUT analytics (no clicks/views) to the Advertiser.
    """
    if not pg_pool or not valkey_client:
        return

    await flush_ram_pers_earnings_to_valkey()
    now_utc = datetime.now(timezone.utc)

    async with pg_pool.acquire() as conn:
        expired_ads = await conn.fetch('''
            SELECT * FROM ad_campaigns
            WHERE approval_status = 'APPROVED'
              AND cleanup_done = FALSE
              AND expires_at <= $1;
        ''', now_utc)

    for ad in expired_ads:
        ad_id = ad["ad_id"]
        advertiser_id = ad["advertiser_id"]
        direct_url = html.escape(ad["target_url"] or "https://t.me")

        sent_entries = await valkey_client.lrange(f"promo:sent_msgs:{ad_id}", 0, -1)

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

        async with pg_pool.acquire() as conn:
            await conn.execute("UPDATE ad_campaigns SET cleanup_done = TRUE WHERE ad_id = $1;", ad_id)

        await valkey_client.delete(
            f"promo:sent_msgs:{ad_id}",
            f"promo:brd_reach:{ad_id}",
            f"promo:pers_reach:{ad_id}"
        )

        # Check if advertiser has any other pending/active campaigns to mention
        async with pg_pool.acquire() as conn:
            other_pending = await conn.fetch('''
                SELECT ad_id, approval_status FROM ad_campaigns
                WHERE advertiser_id = $1
                  AND ad_id != $2
                  AND payment_status = 'PAID'
                  AND (approval_status = 'PENDING' OR (approval_status = 'APPROVED' AND expires_at > NOW()));
            ''', advertiser_id, ad_id)

        extra_note = ""
        if other_pending:
            active_list = ", ".join([f"<code>#AD{r['ad_id']}</code> ({r['approval_status']})" for r in other_pending])
            extra_note = f"\n<blockquote>🟢 <b>Your Other Active / Pending Campaigns:</b> {active_list}</blockquote>"

        end_msg = (
            f"🏁 <b>AD CAMPAIGN #AD{ad_id} HAS ENDED</b> ✨\n\n"
            f"<blockquote>🆔 <b>Campaign ID:</b> <code>#AD{ad_id}</code>\n"
            f"🔗 <b>Redirect URL:</b> <code>{direct_url}</code>\n"
            f"📌 <b>Status:</b> Your promotional ad campaign has officially ended.</blockquote>"
            f"{extra_note}\n"
            f"<i>Use <code>/ad</code> or <code>/set_ad</code> anytime to launch a new campaign!</i>"
        )
        try:
            await bot.send_message(chat_id=advertiser_id, text=end_msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await refresh_active_ads_ram()


async def master_scheduler_worker(bot):
    """Master background scheduler for 4x IST Broadcasts, 5m Valkey Flushes, 7d GC Checks & 10 PM Payouts."""
    global last_executed_slot_key, last_10pm_flush_date
    await asyncio.sleep(10)
    last_5m_ts = time.time()

    while True:
        try:
            now_ist = datetime.now(IST)
            now_ts = time.time()

            if now_ts - last_5m_ts >= 300:
                last_5m_ts = now_ts
                await flush_ram_pers_earnings_to_valkey()
                await refresh_active_ads_ram()
                await verify_pending_7d_gc_bonuses(bot)
                await cleanup_expired_ads(bot)

            if now_ist.hour in BROADCAST_SLOTS_IST and now_ist.minute == 0:
                slot_key = f"{now_ist.strftime('%Y-%m-%d')}_{now_ist.hour}"
                if last_executed_slot_key != slot_key:
                    last_executed_slot_key = slot_key
                    asyncio.create_task(execute_scheduled_broadcast_slot(bot))

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

# Command Handlers (Group 0)
ptb_promo_app.add_handler(CommandHandler("start", cmd_start_handler), group=0)
ptb_promo_app.add_handler(CommandHandler(["ad", "set_ad", "setad", "promote"], cmd_set_ad), group=0)
ptb_promo_app.add_handler(CommandHandler(["rewards", "reward", "earnings", "partner"], cmd_rewards), group=0)
ptb_promo_app.add_handler(CallbackQueryHandler(handle_promo_callbacks), group=0)
ptb_promo_app.add_handler(PreCheckoutQueryHandler(handle_precheckout_query), group=0)
ptb_promo_app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, handle_successful_payment), group=0)
ptb_promo_app.add_handler(ChatMemberHandler(handle_gc_bot_member_update, ChatMemberHandler.MY_CHAT_MEMBER), group=0)

# Group 1 Handler: Runs on ALL Private DM messages (both normal messages AND commands) for Ad Setup + 1/8 Personalized Ads
ptb_promo_app.add_handler(MessageHandler(filters.ChatType.PRIVATE, handle_private_activity_and_ad_setup), group=1)
ptb_promo_app.add_handler(MessageHandler(filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP, track_group_engagement_fast), group=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global valkey_client
    await init_promo_db()
    if VALKEY_URL:
        valkey_client = aioredis.from_url(VALKEY_URL, decode_responses=True)

    await ptb_promo_app.initialize()
    await ptb_promo_app.start()
    await refresh_active_ads_ram()

    rewards_webapp.init_webapp_dependencies(pg_pool, ptb_promo_app.bot, ADMIN_OWNER_ID, BOT_TOKEN)
    sched_task = asyncio.create_task(master_scheduler_worker(ptb_promo_app.bot))

    yield

    sched_task.cancel()
    await flush_ram_pers_earnings_to_valkey()
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
    Verifies payment status with Cashfree API, deducts any applied Earned Balance,
    marks the saved campaign as PAID, and forwards it directly to Admin for Approval!
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
            ad_id = await conn.fetchval('''
                SELECT ad_id FROM ad_campaigns
                WHERE order_id = $1 AND payment_status != 'PAID';
            ''', order_id)

        if ad_id:
            await finalize_ad_payment_and_notify(int(ad_id), "UPI", ptb_promo_app.bot)

        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Cashfree webhook error: {e}")
        return JSONResponse({"status": "error"}, status_code=500)


@app.get("/")
async def health_root():
    return {"service": "Tanya Promo & GC Partner Rewards Engine", "status": "online"}

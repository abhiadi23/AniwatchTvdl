#@cantarellabots
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup
from cantarella.button import Button as InlineKeyboardButton
from config import *

# ─────────────────────────────────────────────
#  Toggle ongoing auto-download
# ─────────────────────────────────────────────

from cantarella.core.database import db

@Client.on_callback_query(filters.regex("^toggle_ongoing$"))
async def on_toggle_ongoing(client: Client, callback_query):
    is_admin = await db.is_admin(callback_query.from_user.id)
    if not is_admin and callback_query.from_user.id != OWNER_ID:
        return await callback_query.answer("❌ You are not authorized to use this.", show_alert=True)

    current_status = await db.get_user_setting(0, "ongoing_enabled", True)
    new_status = not current_status
    await db.set_user_setting(0, "ongoing_enabled", new_status)

    status_icon  = "✅ ᴏɴ"        if new_status else "❌ ᴏꜰꜰ"
    toggle_label = "🔴 ᴛᴜʀɴ ᴏꜰꜰ" if new_status else "🟢 ᴛᴜʀɴ ᴏɴ"
    action       = "ᴇɴᴀʙʟᴇᴅ"     if new_status else "ᴅɪꜱᴀʙʟᴇᴅ"

    mapping_batch_mode = await db.get_user_setting(0, "mapping_batch_mode", True)
    mapping_status_icon = "📦 BATCH" if mapping_batch_mode else "📄 SINGLE"

    active_source = await db.get_user_setting(0, "active_source", "animex")
    source_display = "🌐 ANIMEX" if active_source == "animex" else "📺 MIRURO"

    caption = (
        "<blockquote><b>⚙️ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ</b>\n\n"
        f"<b>📡 ᴏɴɢᴏɪɴɢ ᴀᴜᴛᴏ-ᴅᴏᴡɴʟᴏᴀᴅ:</b> {status_icon}\n"
        f"<b>🔗 ᴍᴀᴘᴘɪɴɢ ᴍᴏᴅᴇ:</b> {mapping_status_icon}\n"
        f"<b>📡 ᴀᴄᴛɪᴠᴇ ꜱᴏᴜʀᴄᴇ:</b> {source_display}\n\n"
        "ᴡʜᴇɴ ᴏɴ, ᴛʜᴇ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴄʜᴇᴄᴋꜱ ꜰᴏʀ ɴᴇᴡ ᴀɴɪᴍᴇ ᴇᴘɪꜱᴏᴅᴇꜱ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ ᴛʜᴇᴍ.</blockquote>"
    )
    mapping_toggle_label = "🔄 ᴍᴀᴘᴘɪɴɢ: ꜱɪɴɢʟᴇ" if mapping_batch_mode else "🔄 ᴍᴀᴘᴘɪɴɢ: ʙᴀᴛᴄʜ"
    source_toggle_label = "🔄 ꜱᴡɪᴛᴄʜ ᴛᴏ ᴍɪʀᴜʀᴏ" if active_source == "animex" else "🔄 ꜱᴡɪᴛᴄʜ ᴛᴏ ᴀɴɪᴍᴇx"

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_label, callback_data="toggle_ongoing")],
        [InlineKeyboardButton(mapping_toggle_label, callback_data="toggle_mapping_mode")],
        [InlineKeyboardButton(source_toggle_label, callback_data="toggle_active_source")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",  callback_data="start")]
    ])
    try:
        await callback_query.edit_message_caption(
            caption=caption,
            reply_markup=buttons,
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass
    await callback_query.answer(f"📡 ᴏɴɢᴏɪɴɢ ᴀᴜᴛᴏ-ᴅᴏᴡɴʟᴏᴀᴅ {action}!", show_alert=True)

@Client.on_callback_query(filters.regex("^toggle_mapping_mode$"))
async def on_toggle_mapping_mode(client: Client, callback_query):
    is_admin = await db.is_admin(callback_query.from_user.id)
    if not is_admin and callback_query.from_user.id != OWNER_ID:
        return await callback_query.answer("❌ You are not authorized to use this.", show_alert=True)

    current_mode = await db.get_user_setting(0, "mapping_batch_mode", True)
    new_mode = not current_mode
    await db.set_user_setting(0, "mapping_batch_mode", new_mode)

    ongoing_enabled = await db.get_user_setting(0, "ongoing_enabled", True)
    status_icon  = "✅ ᴏɴ" if ongoing_enabled else "❌ ᴏꜰꜰ"
    toggle_label = "🔴 ᴛᴜʀɴ ᴏꜰꜰ" if ongoing_enabled else "🟢 ᴛᴜʀɴ ᴏɴ"

    mapping_toggle_label = "🔄 ᴍᴀᴘᴘɪɴɢ: ꜱɪɴɢʟᴇ" if new_mode else "🔄 ᴍᴀᴘᴘɪɴɢ: ʙᴀᴛᴄʜ"
    mapping_status_icon = "📦 BATCH" if new_mode else "📄 SINGLE"
    action = "BATCH" if new_mode else "SINGLE"

    caption = (
        "<blockquote><b>⚙️ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ</b>\n\n"
        f"<b>📡 ᴏɴɢᴏɪɴɢ ᴀᴜᴛᴏ-ᴅᴏᴡɴʟᴏᴀᴅ:</b> {status_icon}\n"
        f"<b>🔗 ᴍᴀᴘᴘɪɴɢ ᴍᴏᴅᴇ:</b> {mapping_status_icon}\n\n"
        "ᴡʜᴇɴ ᴏɴ, ᴛʜᴇ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴄʜᴇᴄᴋꜱ ꜰᴏʀ ɴᴇᴡ ᴀɴɪᴍᴇ ᴇᴘɪꜱᴏᴅᴇꜱ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ ᴛʜᴇᴍ.\n"
        "ᴡʜᴇɴ ᴏꜰꜰ, ᴏɴʟʏ ᴍᴀɴᴜᴀʟ ꜱᴇᴀʀᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ ᴡᴏʀᴋꜱ.</blockquote>"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_label, callback_data="toggle_ongoing")],
        [InlineKeyboardButton(mapping_toggle_label, callback_data="toggle_mapping_mode")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",  callback_data="start")]
    ])

    try:
        if callback_query.message.photo:
            await callback_query.edit_message_caption(
                caption=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML
            )
        else:
            await callback_query.edit_message_text(
                text=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML
            )
    except Exception:
        pass
    await callback_query.answer(f"🔄 Mapping mode set to {action}!", show_alert=True)

@Client.on_callback_query(filters.regex("^toggle_active_source$"))
async def on_toggle_active_source(client: Client, callback_query):
    is_admin = await db.is_admin(callback_query.from_user.id)
    if not is_admin and callback_query.from_user.id != OWNER_ID:
        return await callback_query.answer("❌ You are not authorized to use this.", show_alert=True)

    current_source = await db.get_user_setting(0, "active_source", "animex")
    new_source = "miruro" if current_source == "animex" else "animex"
    await db.set_user_setting(0, "active_source", new_source)

    ongoing_enabled = await db.get_user_setting(0, "ongoing_enabled", True)
    status_icon  = "✅ ᴏɴ" if ongoing_enabled else "❌ ᴏꜰꜰ"
    toggle_label = "🔴 ᴛᴜʀɴ ᴏꜰꜰ" if ongoing_enabled else "🟢 ᴛᴜʀɴ ᴏɴ"

    mapping_batch_mode = await db.get_user_setting(0, "mapping_batch_mode", True)
    mapping_status_icon = "📦 BATCH" if mapping_batch_mode else "📄 SINGLE"
    mapping_toggle_label = "🔄 ᴍᴀᴘᴘɪɴɢ: ꜱɪɴɢʟᴇ" if mapping_batch_mode else "🔄 ᴍᴀᴘᴘɪɴɢ: ʙᴀᴛᴄʜ"

    source_display = "🌐 ANIMEX" if new_source == "animex" else "📺 MIRURO"
    source_toggle_label = "🔄 ꜱᴡɪᴛᴄʜ ᴛᴏ ᴍɪʀᴜʀᴏ" if new_source == "animex" else "🔄 ꜱᴡɪᴛᴄʜ ᴛᴏ ᴀɴɪᴍᴇx"

    caption = (
        "<blockquote><b>⚙️ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ</b>\n\n"
        f"<b>📡 ᴏɴɢᴏɪɴɢ ᴀᴜᴛᴏ-ᴅᴏᴡɴʟᴏᴀᴅ:</b> {status_icon}\n"
        f"<b>🔗 ᴍᴀᴘᴘɪɴɢ ᴍᴏᴅᴇ:</b> {mapping_status_icon}\n"
        f"<b>📡 ᴀᴄᴛɪᴠᴇ ꜱᴏᴜʀᴄᴇ:</b> {source_display}\n\n"
        "ᴡʜᴇɴ ᴏɴ, ᴛʜᴇ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴄʜᴇᴄᴋꜱ ꜰᴏʀ ɴᴇᴡ ᴀɴɪᴍᴇ ᴇᴘɪꜱᴏᴅᴇꜱ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ ᴛʜᴇᴍ.</blockquote>"
    )

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_label, callback_data="toggle_ongoing")],
        [InlineKeyboardButton(mapping_toggle_label, callback_data="toggle_mapping_mode")],
        [InlineKeyboardButton(source_toggle_label, callback_data="toggle_active_source")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",  callback_data="start")]
    ])

    try:
        if callback_query.message.photo:
            await callback_query.edit_message_caption(
                caption=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML
            )
        else:
            await callback_query.edit_message_text(
                text=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML
            )
    except Exception:
        pass
    await callback_query.answer(f"🔄 Active source set to {new_source.upper()}!", show_alert=True)

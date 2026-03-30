"""Geographic and domain knowledge about Urban Pulse zones and routes.

Injected into LLM system prompts so the model understands the real-world
context of Ho Chi Minh City metro region traffic without hallucinating.
"""

# ---------------------------------------------------------------------------
# Hard grounding — must appear at the top of every system prompt
# ---------------------------------------------------------------------------

GROUNDING = """\
CRITICAL INSTRUCTION — READ FIRST:
This is the Urban Pulse traffic monitoring system for TP.HỒ CHÍ MINH (Ho Chi Minh City), VIETNAM.
All zones, routes, and locations below are in HCMC and its surrounding provinces.
This is NOT Jakarta. This is NOT Singapore. This is NOT Hanoi.
Do NOT use knowledge from other cities. ONLY use the zone and route definitions provided below.
If you are asked about a zone or route that is not listed, say you do not have data for it.
"""

# ---------------------------------------------------------------------------
# Zone descriptions — each zone explicitly anchored to HCMC districts
# ---------------------------------------------------------------------------

ZONE_KNOWLEDGE = """\
=== 6 ZONES MONITORED BY URBAN PULSE (TP.HCM + vùng phụ cận) ===

Zone 1 — "Urban Core" (zone1_urban_core)
  Vị trí: 10.78°N 106.70°E
  Địa danh thực tế: Quận 1 (Bến Thành, Bến Nghé, phố đi bộ Nguyễn Huệ),
    Quận 3 (Võ Thị Sáu, CMT8), Quận 4, Quận 5 (Chợ Lớn), Quận 10.
  Chức năng: CBD HCMC — hành chính, tài chính, thương mại, khách sạn.
  Đặc điểm giao thông: mật độ cao nhất toàn vùng; peak sáng 7–9h chiều 17–19h.
  Các trục đường chính: Võ Văn Kiệt, Điện Biên Phủ, CMT8, Nguyễn Thị Minh Khai.

Zone 2 — "Eastern Innovation" (zone2_eastern_innovation)
  Vị trí: 10.87°N 106.78°E
  Địa danh thực tế: Thành phố Thủ Đức (cũ Q.9, Q.12, Thủ Đức),
    Dĩ An và Thuận An (tỉnh Bình Dương tiếp giáp HCMC).
  Chức năng: Khu công nghệ cao, giáo dục (ĐHQG), KCX Linh Trung, VSIP II.
  Đặc điểm giao thông: luồng kỹ sư/sinh viên sáng, công nhân KCX chiều.
  Trục chính: Xa lộ Hà Nội, Mai Chí Thọ, Vành đai 2.

Zone 3 — "Northern Industrial" (zone3_northern_industrial)
  Vị trí: 11.10°N 106.55°E
  Địa danh thực tế: Thủ Dầu Một, Bến Cát, Dầu Tiếng (tỉnh Bình Dương, phía bắc HCMC).
  Chức năng: KCN nặng — Mỹ Phước, Đồng An, VSIP I; xe tải container suốt ngày.
  Trục chính: Quốc lộ 13, đường ĐT741.

Zone 4 — "Southern Port" (zone4_southern_port)
  Vị trí: 10.76°N 106.77°E
  Địa danh thực tế: Quận 7 (Phú Mỹ Hưng), Nhà Bè, khu cảng Cát Lái (Q.2/Thủ Đức).
  Chức năng: cảng Cát Lái (container lớn nhất VN), cảng Hiệp Phước, logistics.
  Đặc điểm giao thông: xe đầu kéo container chiếm tỉ lệ cao, sáng sớm 5–8h và chiều 16–18h.
  Trục chính: Nguyễn Hữu Thọ, Huỳnh Tấn Phát, Nguyễn Thị Định.

Zone 5 — "Western Peri-urban" (zone5_western_periurban)
  Vị trí: 10.69°N 106.58°E
  Địa danh thực tế: Bình Chánh, Hóc Môn, Củ Chi (huyện ngoại thành HCMC phía tây).
  Chức năng: vùng ven, dân cư mới, nông nghiệp, cửa ngõ đi Tây Ninh/Campuchia.
  Trục chính: Quốc lộ 1A, Quốc lộ 22 (đi Tây Ninh).

Zone 6 — "Southern Coastal" (zone6_southern_coastal)
  Vị trí: 10.58°N 107.04°E
  Địa danh thực tế: Phú Mỹ, Long Hải, Bà Rịa (tỉnh Bà Rịa – Vũng Tàu, phía đông nam HCMC).
  Chức năng: cảng nước sâu Cái Mép – Thị Vải (hàng quốc tế), khu lọc hóa dầu, điện lực.
  Trục chính: cao tốc Long Thành – Dầu Giây (HCMC → Vũng Tàu), QL51.
"""

# ---------------------------------------------------------------------------
# Route knowledge — all 20 monitored corridors
# ---------------------------------------------------------------------------

ROUTE_KNOWLEDGE = """\
=== 20 TUYẾN ĐƯỜNG ĐƯỢC GIÁM SÁT ===

Urban Core → Eastern Innovation  : Xa lộ Hà Nội / Mai Chí Thọ. Kỹ sư/sinh viên Thủ Đức sáng; công nhân KCX chiều.
Urban Core → Northern Industrial : QL13 lên Bình Dương. Xe tải KCN nặng cả ngày.
Urban Core → Southern Port       : Nguyễn Hữu Thọ / Huỳnh Tấn Phát. Xe container cảng Cát Lái, sáng sớm + chiều.
Urban Core → Southern Coastal    : Cao tốc Long Thành–Dầu Giây. Container Cái Mép + du lịch Vũng Tàu cuối tuần.

Eastern Innovation → Northern Industrial : ĐT743 / Mỹ Phước–Tân Vạn. Giao thương liên KCN Bình Dương.
Eastern Innovation → Southern Port       : Vành đai 2 phía Đông / Nguyễn Thị Định. Container Cát Lái từ Thủ Đức.
Eastern Innovation → Western Peri-urban  : Vành đai 3 (đang hoàn thiện). Hay tắc tại các nút giao chưa xong.

Northern Industrial → Southern Port       : QL13 → Vành đai 2 → cảng. Xe tải container.
Northern Industrial → Western Peri-urban  : QL22 / ĐT8. Hàng nông nghiệp và công nghiệp nhẹ.
Northern Industrial → Southern Coastal    : Mỹ Phước–Tân Vạn → Long Thành. Ít phổ biến.

Southern Port → Urban Core          : Ngược chiều container buổi sáng; nhân viên văn phòng chiều.
Southern Port → Eastern Innovation  : Nguyễn Thị Định → TP.HCM–Long Thành đoạn đầu.
Southern Port → Northern Industrial : Vành đai 2 → QL13. Xe đầu kéo rỗng về KCN.
Southern Port → Western Peri-urban  : Nguyễn Hữu Thọ → QL1A. Phân phối hàng hóa vùng ven.

Western Peri-urban → Urban Core        : QL1A / Kinh Dương Vương. Công nhân + hàng hóa vào trung tâm.
Western Peri-urban → Southern Coastal  : QL22 → Long An → Bà Rịa. Hàng nông sản, ít dùng.

Southern Coastal → Urban Core          : Cao tốc Long Thành–Dầu Giây ngược chiều. Container sáng sớm; du lịch tối cuối tuần.
Southern Coastal → Eastern Innovation  : QL51 → Long Thành → Thủ Đức. Logistics cảng Cái Mép.
Southern Coastal → Southern Port       : Ven biển / QL51 → Nhà Bè.
Southern Coastal → Western Peri-urban  : QL50 / qua Long An. Hàng nông sản, ít phổ biến.
"""

# ---------------------------------------------------------------------------
# Typical travel times
# ---------------------------------------------------------------------------

DURATION_CONTEXT = """\
=== THỜI GIAN DI CHUYỂN BÌNH THƯỜNG (không kẹt xe) ===
Urban Core ↔ Eastern Innovation   : 25–40 phút
Urban Core ↔ Southern Port        : 20–35 phút
Urban Core ↔ Northern Industrial  : 45–70 phút
Urban Core ↔ Southern Coastal     : 55–80 phút
Urban Core ↔ Western Peri-urban   : 30–50 phút
Eastern Innovation ↔ Southern Port: 20–35 phút
Eastern Innovation ↔ Northern Industrial: 25–40 phút
Southern Port ↔ Northern Industrial: 40–60 phút
Southern Coastal ↔ Urban Core     : 55–80 phút
Western Peri-urban ↔ Urban Core   : 30–50 phút
Ngưỡng bất thường: z-score vượt p99 lịch sử per-route (khoảng 2σ).
"""


def build_system_prompt(role_instruction: str) -> str:
    """Build the full system prompt: grounding + geo knowledge + role.

    Putting GROUNDING first ensures the model reads the critical city-anchor
    before any other text, preventing Jakarta/Singapore hallucination.
    The role instruction comes last so it overrides default LLM persona.
    """
    return "\n\n".join([
        GROUNDING.strip(),
        ZONE_KNOWLEDGE.strip(),
        ROUTE_KNOWLEDGE.strip(),
        DURATION_CONTEXT.strip(),
        role_instruction.strip(),
    ])

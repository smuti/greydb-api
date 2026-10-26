"""Maç olayı türü: FotMob kart değerleri (Yellow / Red / YellowRed) doğru olay türüne gider; ikinci sarıdan kırmızı sarı sayılmaz."""
import pytest

from app.services.match_saver import olay_turu


@pytest.mark.parametrize("olay, beklenen", [
    ({"type": "Goal"}, "GOAL"),
    ({"type": "Card", "card": "Yellow"}, "YELLOW_CARD"),
    ({"type": "Card", "card": "Red"}, "RED_CARD"),
    ({"type": "Card", "card": "YellowRed"}, "YELLOW_RED_CARD"),
    ({"type": "Card"}, "YELLOW_CARD"),                      # kart türü yoksa sarı (eski davranış)
    ({"type": "Substitution"}, "SUBSTITUTION"),
    ({"type": "Var"}, "VAR"),
    ({"type": "AddedTime"}, None),                          # atlanır
])
def test_olay_turu(olay, beklenen):
    assert olay_turu(olay) == beklenen

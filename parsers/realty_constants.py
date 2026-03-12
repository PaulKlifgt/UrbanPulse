"""Константы для парсера недвижимости."""

CIAN_REGIONS = {
    "москва": 1, "московская": 4593,
    "санкт-петербург": 2, "петербург": 2,
    "новосибирск": 4897, "екатеринбург": 4743,
    "казань": 4777, "нижний новгород": 4885,
    "челябинск": 5045, "самара": 4966,
    "омск": 4900, "ростов-на-дону": 4959,
    "уфа": 5024, "красноярск": 4827,
    "воронеж": 4713, "пермь": 4916,
    "волгоград": 4705, "краснодар": 4820,
    "тюмень": 5016, "саратов": 4971,
    "тольятти": 5005, "ижевск": 4763,
    "барнаул": 4680, "иркутск": 4772,
    "ульяновск": 5028, "хабаровск": 5032,
    "ярославль": 5072, "владивосток": 4701,
    "махачкала": 4861, "томск": 5009,
    "оренбург": 4904, "кемерово": 4791,
    "рязань": 4963, "набережные челны": 4882,
    "астрахань": 4674, "пенза": 4912,
    "липецк": 4846, "тула": 5013,
    "калининград": 4780, "сочи": 4998,
}

INTERCEPT_SCRIPT = """
(function() {
    if (window._cianIntercepted) return;
    window._cianIntercepted = true;
    window._cianClusters = null;
    window._cianNetworkLog = [];

    // === Перехват fetch ===
    const _origFetch = window.fetch;
    window.fetch = async function(...args) {
        const url = typeof args[0] === 'string' ? args[0] : (args[0]?.url || '');
        const response = await _origFetch.apply(this, args);
        try {
            if (url.includes('cluster') || url.includes('pin') ||
                url.includes('offer') || url.includes('search')) {
                const clone = response.clone();
                const text = await clone.text();
                window._cianNetworkLog.push({url: url, size: text.length, ts: Date.now()});
                if (url.includes('cluster') || url.includes('map-search') ||
                    url.includes('get-clusters')) {
                    window._cianClusters = text;
                }
                if (!window._cianClusters && text.includes('"offersSerialized"')) {
                    window._cianClusters = text;
                }
                if (!window._cianClusters && text.includes('"clusterOfferIds"')) {
                    window._cianClusters = text;
                }
            }
        } catch(e) {}
        return response;
    };

    // === Перехват XMLHttpRequest ===
    const _origOpen = XMLHttpRequest.prototype.open;
    const _origSend = XMLHttpRequest.prototype.send;

    XMLHttpRequest.prototype.open = function(method, url, ...rest) {
        this._interceptUrl = url;
        return _origOpen.apply(this, [method, url, ...rest]);
    };

    XMLHttpRequest.prototype.send = function(...args) {
        this.addEventListener('load', function() {
            try {
                const url = this._interceptUrl || '';
                if (url.includes('cluster') || url.includes('pin') ||
                    url.includes('offer') || url.includes('search') ||
                    url.includes('map')) {
                    const text = this.responseText || '';
                    window._cianNetworkLog.push({
                        url: url, size: text.length, ts: Date.now(), type: 'xhr'
                    });
                    if (url.includes('cluster') || url.includes('map-search') ||
                        url.includes('get-clusters')) {
                        window._cianClusters = text;
                    }
                    if (!window._cianClusters && text.includes('"clusterOfferIds"')) {
                        window._cianClusters = text;
                    }
                }
            } catch(e) {}
        });
        return _origSend.apply(this, args);
    };
})();
"""
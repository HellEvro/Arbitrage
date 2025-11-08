const socket = io();
const tableBody = document.querySelector("#ranking-table tbody");

// Состояние приложения
const state = {
  opportunities: [],
  frozen: false,
  blacklist: JSON.parse(localStorage.getItem("arbitrage_blacklist") || "[]"),
  whitelist: JSON.parse(localStorage.getItem("arbitrage_whitelist") || "[]"),
  limit: parseInt(localStorage.getItem("arbitrage_limit") || "20"),
  sortBy: localStorage.getItem("arbitrage_sortBy") || "spread_usdt",
  enableBlacklist: localStorage.getItem("arbitrage_enableBlacklist") !== "false",
  enableWhitelist: localStorage.getItem("arbitrage_enableWhitelist") === "true",
  autoSortWhitelist: localStorage.getItem("arbitrage_autoSortWhitelist") !== "false",
};

console.log("Socket.IO initialized:", socket.connected);

socket.on("connect", () => {
  console.log("WebSocket connected");
});

socket.on("disconnect", () => {
  console.log("WebSocket disconnected");
});

socket.on("connect_error", (error) => {
  console.error("WebSocket connection error:", error);
});

const tradeUrlResolvers = {
  bybit: (symbol) => `https://www.bybit.com/trade/spot/${symbol}`,
  mexc: (symbol) => `https://www.mexc.com/exchange/${symbol}`,
  bitget: (symbol) => `https://www.bitget.com/spot/${symbol}`,
  okx: (symbol) => `https://www.okx.com/trade-spot/${symbol}`,
  kucoin: (symbol) => `https://www.kucoin.com/trade/${symbol}`,
};

function createExchangeLink(exchange, symbol) {
  const resolver = tradeUrlResolvers[exchange.toLowerCase()];
  const url = resolver ? resolver(symbol) : `https://${exchange}.com/trade/${symbol}`;
  return `<a href="${url}" target="_blank" rel="noopener noreferrer">${exchange}</a>`;
}

function formatPrice(price) {
  if (price === 0 || !price) return "0.0000";
  if (price < 0.0001) {
    return price.toFixed(8).replace(/\.?0+$/, "");
  } else if (price < 0.01) {
    return price.toFixed(6).replace(/\.?0+$/, "");
  } else if (price < 1) {
    return price.toFixed(4).replace(/\.?0+$/, "");
  } else {
    return price.toFixed(4);
  }
}

function filterOpportunities(opportunities) {
  let filtered = [...opportunities];

  // Применить черный список
  if (state.enableBlacklist && state.blacklist.length > 0) {
    filtered = filtered.filter((opp) => !state.blacklist.includes(opp.symbol.toUpperCase()));
  }

  // Применить белый список
  if (state.enableWhitelist && state.whitelist.length > 0) {
    const whitelistSet = new Set(state.whitelist.map((s) => s.toUpperCase()));
    const whitelisted = filtered.filter((opp) => whitelistSet.has(opp.symbol.toUpperCase()));
    const others = filtered.filter((opp) => !whitelistSet.has(opp.symbol.toUpperCase()));
    
    if (state.autoSortWhitelist) {
      filtered = [...whitelisted, ...others];
    } else {
      filtered = whitelisted.length > 0 ? whitelisted : filtered;
    }
  }

  return filtered;
}

function sortOpportunities(opportunities) {
  const sorted = [...opportunities];
  const sortBy = state.sortBy;
  const isDescending = sortBy === "spread_usdt" || sortBy === "spread_pct";

  sorted.sort((a, b) => {
    let aVal, bVal;
    
    switch (sortBy) {
      case "spread_usdt":
        aVal = a.spread_usdt || 0;
        bVal = b.spread_usdt || 0;
        break;
      case "spread_pct":
        aVal = a.spread_pct || 0;
        bVal = b.spread_pct || 0;
        break;
      case "symbol":
        aVal = (a.symbol || "").toUpperCase();
        bVal = (b.symbol || "").toUpperCase();
        return aVal.localeCompare(bVal);
      case "buy_price":
        aVal = a.buy_price || 0;
        bVal = b.buy_price || 0;
        break;
      case "sell_price":
        aVal = a.sell_price || 0;
        bVal = b.sell_price || 0;
        break;
      default:
        return 0;
    }

    if (isDescending) {
      return bVal - aVal;
    } else {
      return aVal - bVal;
    }
  });

  return sorted;
}

function limitOpportunities(opportunities) {
  if (state.limit === "all") {
    return opportunities; // Вернуть все без ограничений
  }
  
  if (state.limit === "custom") {
    const customLimit = parseInt(document.getElementById("custom-limit")?.value || "20");
    return opportunities.slice(0, customLimit);
  }
  
  // Числовое значение (10, 20, 50, 100)
  const limit = parseInt(state.limit) || 20;
  return opportunities.slice(0, limit);
}

function processOpportunities(opportunities) {
  let processed = filterOpportunities(opportunities);
  console.log("After filtering:", processed.length);
  
  processed = sortOpportunities(processed);
  console.log("After sorting:", processed.length);
  
  processed = limitOpportunities(processed);
  console.log("After limiting:", processed.length, "limit:", state.limit);
  
  return processed;
}

function renderOpportunities(opportunities) {
  console.log("Rendering opportunities:", opportunities?.length || 0);
  
    if (!Array.isArray(opportunities) || opportunities.length === 0) {
    console.warn("No opportunities to render:", opportunities);
    if (tableBody) {
      tableBody.innerHTML = "<tr><td colspan='12' style='text-align: center;'>Нет данных</td></tr>";
    }
    return;
  }

  if (!tableBody) {
    console.error("Table body not found!");
    return;
  }

  const processed = processOpportunities(opportunities);
  
  tableBody.innerHTML = processed
    .map((opp) => {
      const grossProfit = opp.gross_profit_usdt || 0;
      const totalFees = opp.total_fees_usdt || 0;
      const netProfit = opp.spread_usdt || 0;
      
      return `
        <tr>
          <td><strong>${opp.symbol}</strong></td>
          <td>${createExchangeLink(opp.buy_exchange, opp.buy_symbol || opp.symbol)}</td>
          <td>${formatPrice(opp.buy_price)}</td>
          <td><span class="fee-badge">${opp.buy_fee_pct?.toFixed(3) || "0.100"}%</span></td>
          <td>${createExchangeLink(opp.sell_exchange, opp.sell_symbol || opp.symbol)}</td>
          <td>${formatPrice(opp.sell_price)}</td>
          <td><span class="fee-badge">${opp.sell_fee_pct?.toFixed(3) || "0.100"}%</span></td>
          <td><span class="gross-profit">${grossProfit.toFixed(2)}</span></td>
          <td><span class="fees-amount">-${totalFees.toFixed(2)}</span></td>
          <td><strong class="profit">${netProfit.toFixed(2)}</strong></td>
          <td>${opp.spread_pct.toFixed(3)}%</td>
          <td>${new Date(opp.timestamp_ms).toLocaleTimeString()}</td>
        </tr>
      `;
    })
    .join("");
}

socket.on("opportunities", (data) => {
  if (state.frozen) {
    return; // Не обновляем, если зафиксировано
  }
  console.log("Received opportunities via WebSocket:", data?.length || 0);
  state.opportunities = data || [];
  renderOpportunities(state.opportunities);
});

// Управление табами
document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const tabName = btn.dataset.tab;
    
    // Обновить активные табы
    document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-pane").forEach((p) => p.classList.remove("active"));
    
    btn.classList.add("active");
    document.getElementById(`tab-${tabName}`).classList.add("active");
  });
});

// Управление лимитом
const limitSelect = document.getElementById("limit-select");
const customLimitInput = document.getElementById("custom-limit");

limitSelect.addEventListener("change", (e) => {
  const value = e.target.value;
  state.limit = value;
  localStorage.setItem("arbitrage_limit", value);
  
  if (value === "custom") {
    customLimitInput.style.display = "inline-block";
    const savedCustomLimit = localStorage.getItem("arbitrage_customLimit") || "20";
    customLimitInput.value = savedCustomLimit;
  } else {
    customLimitInput.style.display = "none";
  }
  
  console.log("Limit changed to:", value, "Current opportunities:", state.opportunities.length);
  renderOpportunities(state.opportunities);
});

customLimitInput.addEventListener("input", (e) => {
  const value = parseInt(e.target.value) || 20;
  localStorage.setItem("arbitrage_customLimit", value.toString());
  console.log("Custom limit changed to:", value);
  renderOpportunities(state.opportunities);
});

// Также обрабатываем изменение через события change и keyup для более быстрой реакции
customLimitInput.addEventListener("change", (e) => {
  const value = parseInt(e.target.value) || 20;
  localStorage.setItem("arbitrage_customLimit", value.toString());
  console.log("Custom limit changed (change event):", value);
  renderOpportunities(state.opportunities);
});

// Управление сортировкой
const sortSelect = document.getElementById("sort-select");
sortSelect.value = state.sortBy;
sortSelect.addEventListener("change", (e) => {
  state.sortBy = e.target.value;
  localStorage.setItem("arbitrage_sortBy", state.sortBy);
  renderOpportunities(state.opportunities);
});

// Фиксация
const freezeBtn = document.getElementById("freeze-btn");
const freezeStatus = document.getElementById("freeze-status");

freezeBtn.addEventListener("click", () => {
  state.frozen = !state.frozen;
  if (state.frozen) {
    freezeBtn.textContent = "▶ Возобновить";
    freezeBtn.classList.remove("btn-secondary");
    freezeBtn.classList.add("btn-success");
    freezeStatus.textContent = " (зафиксировано)";
    freezeStatus.style.color = "#4ade80";
  } else {
    freezeBtn.textContent = "⏸ Зафиксировать";
    freezeBtn.classList.remove("btn-success");
    freezeBtn.classList.add("btn-secondary");
    freezeStatus.textContent = "";
    renderOpportunities(state.opportunities);
  }
});

// Управление черным списком
function renderBlacklist() {
  const container = document.getElementById("blacklist-items");
  if (!container) return;
  
  if (state.blacklist.length === 0) {
    container.innerHTML = "<p class='empty-message'>Черный список пуст</p>";
    return;
  }
  
  container.innerHTML = state.blacklist
    .map(
      (symbol) => `
    <div class="list-item">
      <span>${symbol}</span>
      <button class="btn btn-small btn-danger" onclick="removeFromBlacklist('${symbol}')">Удалить</button>
    </div>
  `
    )
    .join("");
}

function addToBlacklist(symbol) {
  const upperSymbol = symbol.toUpperCase().trim();
  if (!upperSymbol) return;
  
  if (!state.blacklist.includes(upperSymbol)) {
    state.blacklist.push(upperSymbol);
    localStorage.setItem("arbitrage_blacklist", JSON.stringify(state.blacklist));
    renderBlacklist();
    renderOpportunities(state.opportunities);
  }
}

function removeFromBlacklist(symbol) {
  state.blacklist = state.blacklist.filter((s) => s !== symbol);
  localStorage.setItem("arbitrage_blacklist", JSON.stringify(state.blacklist));
  renderBlacklist();
  renderOpportunities(state.opportunities);
}

window.removeFromBlacklist = removeFromBlacklist;

document.getElementById("add-blacklist-btn").addEventListener("click", () => {
  const input = document.getElementById("blacklist-input");
  addToBlacklist(input.value);
  input.value = "";
});

document.getElementById("blacklist-input").addEventListener("keypress", (e) => {
  if (e.key === "Enter") {
    addToBlacklist(e.target.value);
    e.target.value = "";
  }
});

// Управление белым списком
function renderWhitelist() {
  const container = document.getElementById("whitelist-items");
  if (!container) return;
  
  if (state.whitelist.length === 0) {
    container.innerHTML = "<p class='empty-message'>Белый список пуст</p>";
    return;
  }
  
  container.innerHTML = state.whitelist
    .map(
      (symbol) => `
    <div class="list-item">
      <span>${symbol}</span>
      <button class="btn btn-small btn-danger" onclick="removeFromWhitelist('${symbol}')">Удалить</button>
    </div>
  `
    )
    .join("");
}

function addToWhitelist(symbol) {
  const upperSymbol = symbol.toUpperCase().trim();
  if (!upperSymbol) return;
  
  if (!state.whitelist.includes(upperSymbol)) {
    state.whitelist.push(upperSymbol);
    localStorage.setItem("arbitrage_whitelist", JSON.stringify(state.whitelist));
    renderWhitelist();
    renderOpportunities(state.opportunities);
  }
}

function removeFromWhitelist(symbol) {
  state.whitelist = state.whitelist.filter((s) => s !== symbol);
  localStorage.setItem("arbitrage_whitelist", JSON.stringify(state.whitelist));
  renderWhitelist();
  renderOpportunities(state.opportunities);
}

window.removeFromWhitelist = removeFromWhitelist;

document.getElementById("add-whitelist-btn").addEventListener("click", () => {
  const input = document.getElementById("whitelist-input");
  addToWhitelist(input.value);
  input.value = "";
});

document.getElementById("whitelist-input").addEventListener("keypress", (e) => {
  if (e.key === "Enter") {
    addToWhitelist(e.target.value);
    e.target.value = "";
  }
});

// Настройки
document.getElementById("enable-blacklist").checked = state.enableBlacklist;
document.getElementById("enable-whitelist").checked = state.enableWhitelist;
document.getElementById("auto-sort-whitelist").checked = state.autoSortWhitelist;

document.getElementById("enable-blacklist").addEventListener("change", (e) => {
  state.enableBlacklist = e.target.checked;
  localStorage.setItem("arbitrage_enableBlacklist", state.enableBlacklist.toString());
  renderOpportunities(state.opportunities);
});

document.getElementById("enable-whitelist").addEventListener("change", (e) => {
  state.enableWhitelist = e.target.checked;
  localStorage.setItem("arbitrage_enableWhitelist", state.enableWhitelist.toString());
  renderOpportunities(state.opportunities);
});

document.getElementById("auto-sort-whitelist").addEventListener("change", (e) => {
  state.autoSortWhitelist = e.target.checked;
  localStorage.setItem("arbitrage_autoSortWhitelist", state.autoSortWhitelist.toString());
  renderOpportunities(state.opportunities);
});

document.getElementById("clear-all-btn").addEventListener("click", () => {
  if (confirm("Вы уверены, что хотите очистить все списки?")) {
    state.blacklist = [];
    state.whitelist = [];
    localStorage.setItem("arbitrage_blacklist", JSON.stringify([]));
    localStorage.setItem("arbitrage_whitelist", JSON.stringify([]));
    renderBlacklist();
    renderWhitelist();
    renderOpportunities(state.opportunities);
  }
});

// Инициализация
async function fetchInitial() {
  try {
    console.log("Fetching initial data from /api/ranking");
    const response = await fetch("/api/ranking");
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    console.log("Initial data received:", data?.length || 0, "opportunities");
    state.opportunities = data || [];
    renderOpportunities(state.opportunities);
  } catch (error) {
    console.error("Failed to load initial data", error);
    if (tableBody) {
      tableBody.innerHTML = `<tr><td colspan='12' style='text-align: center; color: red;'>Ошибка загрузки данных: ${error.message}</td></tr>`;
    }
  }
}

// Инициализация UI
if (state.limit === "custom") {
  customLimitInput.style.display = "inline-block";
  customLimitInput.value = localStorage.getItem("arbitrage_customLimit") || "20";
} else {
  customLimitInput.style.display = "none";
}
limitSelect.value = state.limit;

renderBlacklist();
renderWhitelist();
fetchInitial();

const socket = io();
const tableBody = document.querySelector("#ranking-table tbody");

// Состояние приложения
const state = {
  opportunities: [],
  frozen: false,
  blacklist: JSON.parse(localStorage.getItem("arbitrage_blacklist") || "[]"),
  whitelist: JSON.parse(localStorage.getItem("arbitrage_whitelist") || "[]"),
  limit: localStorage.getItem("arbitrage_limit") || "20", // Всегда строка для совместимости с select
  sortBy: localStorage.getItem("arbitrage_sortBy") || "spread_usdt",
  enableBlacklist: localStorage.getItem("arbitrage_enableBlacklist") !== "false",
  enableWhitelist: localStorage.getItem("arbitrage_enableWhitelist") === "true",
  autoSortWhitelist: localStorage.getItem("arbitrage_autoSortWhitelist") !== "false",
  expandedGroups: JSON.parse(localStorage.getItem("arbitrage_expandedGroups") || "[]"), // Раскрытые группы
  searchQuery: "", // Поисковый запрос
  allGroupsExpandedMode: localStorage.getItem("arbitrage_allGroupsExpandedMode") === "true", // Режим: все раскрыты (true) или все закрыты (false)
  hasReceivedData: false, // Получены ли данные хотя бы раз
  exchangeStatuses: {}, // Статусы бирж
};

console.log("Socket.IO initialized:", socket.connected);

socket.on("connect", () => {
  console.log("WebSocket connected");
  // При подключении WebSocket перерисовываем таблицу, чтобы обновить сообщение о статусе
  renderOpportunities(state.opportunities);
});

socket.on("disconnect", () => {
  console.log("WebSocket disconnected");
});

socket.on("connect_error", (error) => {
  console.error("WebSocket connection error:", error);
});

const tradeUrlResolvers = {
  bybit: (symbol) => `https://www.bybit.com/ru-RU/trade/spot/${symbol}`,
  mexc: (symbol) => `https://www.mexc.com/ru-RU/exchange/${symbol}`,
  bitget: (symbol) => `https://www.bitget.com/ru-RU/spot/${symbol}`,
  okx: (symbol) => `https://www.okx.com/ru-RU/trade-spot/${symbol}`,
  kucoin: (symbol) => `https://www.kucoin.com/ru-RU/trade/${symbol}`,
};

function formatSymbol(symbol) {
  // Убираем USDT из конца символа для отображения
  if (symbol && symbol.toUpperCase().endsWith("USDT")) {
    return symbol.slice(0, -4);
  }
  return symbol;
}

function createExchangeLink(exchange, symbol) {
  // Разделяем символ на BASE/USDT для правильных ссылок
  let urlSymbol = symbol;
  if (symbol && symbol.toUpperCase().endsWith("USDT")) {
    const base = symbol.slice(0, -4);
    const quote = "USDT";
    
    // Формируем правильные ссылки для каждой биржи
    switch (exchange.toLowerCase()) {
      case "bybit":
        urlSymbol = `${base}/${quote}`;
        break;
      case "mexc":
        urlSymbol = `${base}_${quote}`;
        break;
      case "bitget":
        urlSymbol = symbol; // Bitget использует формат без разделителя
        break;
      case "okx":
        urlSymbol = `${base}-${quote}`;
        break;
      case "kucoin":
        urlSymbol = `${base}-${quote}`;
        break;
      default:
        urlSymbol = symbol;
    }
  }
  
  const resolver = tradeUrlResolvers[exchange.toLowerCase()];
  const url = resolver ? resolver(urlSymbol) : `https://${exchange}.com/trade/${urlSymbol}`;
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

  // Применить поиск по символу
  const searchQuery = state.searchQuery?.trim().toUpperCase() || "";
  if (searchQuery) {
    filtered = filtered.filter((opp) => {
      const symbol = opp.symbol.toUpperCase();
      const displaySymbol = formatSymbol(symbol).toUpperCase();
      // Ищем в полном символе (например, BTCUSDT) и в отображаемом символе (BTC)
      return symbol.includes(searchQuery) || displaySymbol.includes(searchQuery);
    });
    console.log("After search filter:", filtered.length, "query:", searchQuery);
  }

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
  const limit = String(state.limit); // Убеждаемся что это строка
  
  if (limit === "all") {
    return opportunities; // Вернуть все без ограничений
  }
  
  if (limit === "custom") {
    const customLimit = parseInt(document.getElementById("custom-limit")?.value || "20", 10);
    return opportunities.slice(0, customLimit);
  }
  
  // Числовое значение (10, 20, 50, 100)
  const numLimit = parseInt(limit, 10) || 20;
  return opportunities.slice(0, numLimit);
}

function groupOpportunitiesBySymbol(opportunities) {
  const groups = {};
  for (const opp of opportunities) {
    if (!groups[opp.symbol]) {
      groups[opp.symbol] = [];
    }
    groups[opp.symbol].push(opp);
  }
  // Сортируем возможности внутри каждой группы по прибыли
  for (const symbol in groups) {
    groups[symbol].sort((a, b) => (b.spread_usdt || 0) - (a.spread_usdt || 0));
  }
  return groups;
}

function processOpportunities(opportunities) {
  console.log("Processing opportunities:", opportunities.length, "total");
  
  let processed = filterOpportunities(opportunities);
  console.log("After filtering:", processed.length);
  
  processed = sortOpportunities(processed);
  console.log("After sorting:", processed.length);
  
  // Группируем по символам ПЕРЕД ограничением
  // Это позволит правильно применять лимит к группам монет, а не к отдельным возможностям
  let grouped = groupOpportunitiesBySymbol(processed);
  console.log("Grouped into", Object.keys(grouped).length, "symbol groups");
  
  // Применяем лимит к количеству групп (монет), а не к количеству возможностей
  if (state.limit !== "all") {
    const limit = state.limit === "custom" 
      ? parseInt(document.getElementById("custom-limit")?.value || "20", 10)
      : parseInt(state.limit, 10) || 20;
    
    // Сортируем группы по максимальной прибыли и берем топ N
    const sortedGroups = Object.keys(grouped).sort((a, b) => {
      const maxA = Math.max(...grouped[a].map(o => o.spread_usdt || 0));
      const maxB = Math.max(...grouped[b].map(o => o.spread_usdt || 0));
      return maxB - maxA;
    });
    
    const limitedGroups = {};
    for (let i = 0; i < Math.min(limit, sortedGroups.length); i++) {
      limitedGroups[sortedGroups[i]] = grouped[sortedGroups[i]];
    }
    grouped = limitedGroups;
    console.log("After limiting groups:", Object.keys(grouped).length, "limit:", limit);
  }
  
  return grouped;
}

function renderOpportunities(opportunities) {
  console.log("Rendering opportunities:", opportunities?.length || 0);
  
  if (!Array.isArray(opportunities) || opportunities.length === 0) {
    console.warn("No opportunities to render:", opportunities);
    if (tableBody) {
      // Определяем состояние для отображения сообщения
      let message = "Нет данных";
      
      // Проверяем статус бирж
      const statuses = state.exchangeStatuses || {};
      const exchangeNames = Object.keys(statuses);
      
      if (exchangeNames.length > 0) {
        // Проверяем, все ли биржи оффлайн (0 монет или не подключены)
        const allOffline = exchangeNames.every(name => {
          const status = statuses[name];
          return !status.connected || (status.quote_count === 0);
        });
        
        // Проверяем, есть ли хотя бы одна биржа с данными
        const hasAnyData = exchangeNames.some(name => {
          const status = statuses[name];
          return status.connected && status.quote_count > 0;
        });
        
        if (allOffline) {
          message = "Сервера недоступны";
        } else if (!hasAnyData || !state.hasReceivedData) {
          // Если биржи подключены, но данных еще нет - идет загрузка
          message = "Идет загрузка данных...";
        }
      } else if (!state.hasReceivedData && socket.connected) {
        // Если статусы бирж еще не загружены, но WebSocket подключен - идет загрузка
        message = "Идет загрузка данных...";
      }
      
      tableBody.innerHTML = `<tr><td colspan='12' style='text-align: center;'>${message}</td></tr>`;
    }
    return;
  }

  if (!tableBody) {
    console.error("Table body not found!");
    return;
  }

  const processed = processOpportunities(opportunities);
  
  // processed теперь объект с группами по символам
  const symbols = Object.keys(processed).sort((a, b) => {
    // Сортируем группы по максимальной прибыли в группе
    const maxA = Math.max(...processed[a].map(o => o.spread_usdt || 0));
    const maxB = Math.max(...processed[b].map(o => o.spread_usdt || 0));
    return maxB - maxA;
  });
  
  // Применяем режим "все раскрыты/закрыты" к новым монетам
  if (state.allGroupsExpandedMode !== undefined) {
    // Если режим установлен, применяем его к новым монетам
    for (const symbol of symbols) {
      if (state.allGroupsExpandedMode) {
        // Режим "все раскрыты" - добавляем новые монеты в expandedGroups
        if (!state.expandedGroups.includes(symbol)) {
          state.expandedGroups.push(symbol);
        }
      } else {
        // Режим "все закрыты" - удаляем новые монеты из expandedGroups
        const index = state.expandedGroups.indexOf(symbol);
        if (index !== -1) {
          state.expandedGroups.splice(index, 1);
        }
      }
    }
    // Сохраняем обновленный список
    localStorage.setItem("arbitrage_expandedGroups", JSON.stringify(state.expandedGroups));
  }
  
  let html = "";
  for (const symbol of symbols) {
    const opps = processed[symbol];
    const maxProfit = Math.max(...opps.map(o => o.spread_usdt || 0));
    const groupId = `group-${symbol}`;
    // Проверяем состояние: если режим установлен, используем его, иначе проверяем expandedGroups
    const isExpanded = state.allGroupsExpandedMode !== undefined 
      ? state.allGroupsExpandedMode 
      : (state.expandedGroups?.includes(symbol) ?? true); // По умолчанию раскрыто
    
    // Заголовок группы
    const displaySymbol = formatSymbol(symbol); // Убираем USDT для отображения
    html += `
      <tr class="symbol-group-header" data-symbol="${symbol}" onclick="toggleGroup('${symbol}')">
        <td colspan="12" style="background-color: #21262d; cursor: pointer; user-select: none;">
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span class="group-toggle-icon" style="font-size: 0.9rem;">${isExpanded ? '▼' : '▶'}</span>
            <strong style="font-size: 1.1rem;">${displaySymbol}</strong>
            <span style="color: #8b949e; font-size: 0.9rem;">
              (${opps.length} ${opps.length === 1 ? 'возможность' : opps.length < 5 ? 'возможности' : 'возможностей'}, 
              макс. прибыль: <span class="profit">${maxProfit.toFixed(2)} USDT</span>)
            </span>
          </div>
        </td>
      </tr>
    `;
    
    // Строки с возможностями (всегда рендерим, но скрываем через display если свернуто)
    for (const opp of opps) {
      const grossProfit = opp.gross_profit_usdt || 0;
      const totalFees = opp.total_fees_usdt || 0;
      const netProfit = opp.spread_usdt || 0;
      
      html += `
        <tr class="symbol-group-row" data-group="${symbol}" style="background-color: #161b22; display: ${isExpanded ? '' : 'none'};">
          <td style="padding-left: 2rem;">→</td>
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
    }
  }
  
  if (html === "") {
    // Определяем состояние для отображения сообщения
    let message = "Нет данных";
    
    // Проверяем статус бирж
    const statuses = state.exchangeStatuses || {};
    const exchangeNames = Object.keys(statuses);
    
    if (exchangeNames.length > 0) {
      // Проверяем, все ли биржи оффлайн (0 монет или не подключены)
      const allOffline = exchangeNames.every(name => {
        const status = statuses[name];
        return !status.connected || (status.quote_count === 0);
      });
      
      if (allOffline) {
        message = "Сервера недоступны";
      }
    }
    
    html = `<tr><td colspan='12' style='text-align: center;'>${message}</td></tr>`;
  }
  
  tableBody.innerHTML = html;
  
  // Обновляем иконки раскрытия после рендеринга
  updateGroupIcons();
}

function toggleGroup(symbol) {
  console.log("toggleGroup called for symbol:", symbol);
  if (!state.expandedGroups) {
    state.expandedGroups = [];
  }
  
  // Если режим "все раскрыты" или "все закрыты" активен, сбрасываем его при ручном переключении
  if (state.allGroupsExpandedMode !== undefined) {
    // Сбрасываем режим - теперь управление индивидуальное
    state.allGroupsExpandedMode = undefined;
    localStorage.removeItem("arbitrage_allGroupsExpandedMode");
  }
  
  const index = state.expandedGroups.indexOf(symbol);
  if (index === -1) {
    // Добавляем символ - разворачиваем группу
    state.expandedGroups.push(symbol);
    console.log("Expanding group:", symbol, "expandedGroups:", state.expandedGroups);
  } else {
    // Удаляем символ - сворачиваем группу
    state.expandedGroups.splice(index, 1);
    console.log("Collapsing group:", symbol, "expandedGroups:", state.expandedGroups);
  }
  localStorage.setItem("arbitrage_expandedGroups", JSON.stringify(state.expandedGroups));
  // Обновляем отображение через updateGroupIcons вместо полного рендера
  updateGroupIcons();
}

function toggleAllGroups() {
  const processed = processOpportunities(state.opportunities);
  const allSymbols = Object.keys(processed);
  
  if (!state.expandedGroups) {
    state.expandedGroups = [];
  }
  
  // Проверяем, все ли группы раскрыты (по умолчанию считаем раскрытыми, если их нет в списке)
  const allExpanded = allSymbols.length > 0 && allSymbols.every(symbol => state.expandedGroups.includes(symbol));
  
  if (allExpanded) {
    // Сворачиваем все - сохраняем режим "все закрыты"
    state.expandedGroups = [];
    state.allGroupsExpandedMode = false; // Режим: все закрыты
    updateCollapseButton(false);
  } else {
    // Разворачиваем все - сохраняем режим "все раскрыты"
    state.expandedGroups = [...allSymbols];
    state.allGroupsExpandedMode = true; // Режим: все раскрыты
    updateCollapseButton(true);
  }
  
  localStorage.setItem("arbitrage_expandedGroups", JSON.stringify(state.expandedGroups));
  localStorage.setItem("arbitrage_allGroupsExpandedMode", state.allGroupsExpandedMode.toString());
  // Используем updateGroupIcons вместо полного рендера для быстрого обновления
  updateGroupIcons();
}

function updateCollapseButton(allExpanded) {
  const btn = document.getElementById("collapse-all-btn");
  if (btn) {
    const icon = btn.querySelector(".collapse-all-icon");
    if (icon) {
      if (allExpanded) {
        icon.textContent = "▲";
        btn.classList.remove("btn-secondary");
        btn.classList.add("btn-success");
        btn.title = "Развернуть все монеты";
      } else {
        icon.textContent = "▼";
        btn.classList.remove("btn-success");
        btn.classList.add("btn-secondary");
        btn.title = "Свернуть все монеты";
      }
    }
  }
}

function updateGroupIcons() {
  console.log("updateGroupIcons called, expandedGroups:", state.expandedGroups, "allGroupsExpandedMode:", state.allGroupsExpandedMode);
  document.querySelectorAll(".symbol-group-header").forEach(header => {
    const symbol = header.dataset.symbol;
    if (!symbol) {
      console.warn("Header without symbol:", header);
      return;
    }
    // Проверяем состояние: если режим установлен, используем его, иначе проверяем expandedGroups
    const isExpanded = state.allGroupsExpandedMode !== undefined 
      ? state.allGroupsExpandedMode 
      : (state.expandedGroups?.includes(symbol) ?? true);
    console.log(`Group ${symbol}: isExpanded=${isExpanded}`);
    
    const icon = header.querySelector(".group-toggle-icon");
    if (icon) {
      icon.textContent = isExpanded ? '▼' : '▶';
    }
    
    // Показываем/скрываем строки группы
    const rows = document.querySelectorAll(`tr.symbol-group-row[data-group="${symbol}"]`);
    console.log(`Found ${rows.length} rows for group ${symbol}`);
    rows.forEach(row => {
      row.style.display = isExpanded ? "" : "none";
    });
  });
  
  // Обновляем кнопку "Свернуть все"
  const processed = processOpportunities(state.opportunities);
  const allSymbols = Object.keys(processed);
  // Если режим установлен, используем его, иначе проверяем expandedGroups
  const allExpanded = state.allGroupsExpandedMode !== undefined
    ? state.allGroupsExpandedMode
    : (allSymbols.length > 0 && allSymbols.every(s => state.expandedGroups?.includes(s) ?? true));
  updateCollapseButton(allExpanded);
}

// Делаем функции доступными глобально
window.toggleGroup = toggleGroup;
window.toggleAllGroups = toggleAllGroups;

socket.on("opportunities", (data) => {
  if (state.frozen) {
    return; // Не обновляем, если зафиксировано
  }
  console.log("Received opportunities via WebSocket:", data?.length || 0);
  state.opportunities = data || [];
  state.hasReceivedData = true; // Отмечаем, что данные получены
  renderOpportunities(state.opportunities);
});

// Обработка статуса бирж
socket.on("exchange_status", (statuses) => {
  console.log("Received exchange status:", statuses);
  state.exchangeStatuses = statuses || {}; // Сохраняем статусы в state
  renderExchangeStatus(statuses);
  // Перерисовываем таблицу, чтобы обновить сообщение о статусе
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

// Управление поиском
const searchInput = document.getElementById("search-input");
if (searchInput) {
  // Восстанавливаем значение из состояния
  searchInput.value = state.searchQuery || "";
  
  searchInput.addEventListener("input", (e) => {
    state.searchQuery = e.target.value;
    console.log("Search query changed to:", state.searchQuery);
    renderOpportunities(state.opportunities);
  });
  
  searchInput.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      searchInput.value = "";
      state.searchQuery = "";
      console.log("Search cleared");
      renderOpportunities(state.opportunities);
    }
  });
}

// Управление лимитом
const limitSelect = document.getElementById("limit-select");
const customLimitInput = document.getElementById("custom-limit");

limitSelect.addEventListener("change", (e) => {
  const value = String(e.target.value); // Убеждаемся что это строка
  state.limit = value;
  localStorage.setItem("arbitrage_limit", value);
  
  if (value === "custom") {
    customLimitInput.style.display = "inline-block";
    const savedCustomLimit = localStorage.getItem("arbitrage_customLimit") || "20";
    customLimitInput.value = savedCustomLimit;
  } else {
    customLimitInput.style.display = "none";
  }
  
  console.log("Limit changed to:", value, "Current opportunities:", state.opportunities.length, "Total available:", state.opportunities.length);
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

// Рендеринг статуса бирж
function renderExchangeStatus(statuses) {
  const container = document.getElementById("exchange-status-list");
  if (!container) return;
  
  if (!statuses || Object.keys(statuses).length === 0) {
    container.innerHTML = "<p style='color: #8b949e;'>Загрузка статуса бирж...</p>";
    return;
  }
  
  const sortedExchanges = Object.values(statuses).sort((a, b) => {
    // Сначала подключенные, потом отключенные
    if (a.connected !== b.connected) {
      return a.connected ? -1 : 1;
    }
    return a.name.localeCompare(b.name);
  });
  
  container.innerHTML = sortedExchanges
    .map((status) => {
      const age = status.last_update_ms
        ? Math.floor((Date.now() - status.last_update_ms) / 1000)
        : null;
      const ageText = age !== null && age < 60 ? `${age}с` : age !== null ? `${Math.floor(age / 60)}м` : "";
      
      // quote_count теперь показывает количество уникальных монет с котировками
      const coinCount = status.quote_count || 0;
      const formattedCount = coinCount.toLocaleString('ru-RU');
      
      // Формируем детали статуса
      let details = [];
      if (coinCount > 0) {
        details.push(`Монет: ${formattedCount}`);
      } else {
        details.push("Нет данных");
      }
      if (ageText) {
        details.push(`Обновлено: ${ageText} назад`);
      }
      if (status.error_count > 0) {
        details.push(`Ошибок: ${status.error_count}`);
      }
      if (status.last_error) {
        details.push(`Ошибка: ${status.last_error.substring(0, 50)}`);
      }
      
      const tooltip = `${status.connected ? '✅ Подключено' : '❌ Отключено'} • ${details.join(' • ')}`;
      
      return `
        <div class="exchange-status-item" title="${tooltip}">
          <div class="exchange-status-indicator ${status.connected ? "connected" : "disconnected"}"></div>
          <span class="exchange-status-name">${status.name.toUpperCase()}</span>
          <span class="exchange-status-details">
            ${status.connected ? '✅' : '❌'} 
            ${coinCount > 0 ? `${formattedCount} монет` : '0 монет'} 
            ${ageText ? ` • ${ageText}` : ''}
            ${status.error_count > 0 ? ` • ⚠️${status.error_count}` : ''}
          </span>
        </div>
      `;
    })
    .join("");
}

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
    // Обновляем данные только если WebSocket еще не подключен или данных еще нет
    // Это предотвращает перезапись данных, которые уже пришли через WebSocket
    if (!state.hasReceivedData || state.opportunities.length === 0) {
      state.opportunities = data || [];
      state.hasReceivedData = true; // Отмечаем, что данные получены
      renderOpportunities(state.opportunities);
    }
    
    // Загрузить статус бирж (всегда обновляем статус, так как он может измениться)
    try {
      const statusResponse = await fetch("/api/exchange-status");
      if (statusResponse.ok) {
        const statusData = await statusResponse.json();
        state.exchangeStatuses = statusData || {}; // Сохраняем статусы в state
        renderExchangeStatus(statusData);
        // Перерисовываем таблицу, чтобы обновить сообщение о статусе
        renderOpportunities(state.opportunities);
      }
    } catch (e) {
      console.warn("Failed to load exchange status:", e);
    }
  } catch (error) {
    console.error("Failed to load initial data", error);
    // Не показываем ошибку, если WebSocket уже подключен и работает
    if (!socket.connected && tableBody) {
      tableBody.innerHTML = `<tr><td colspan='12' style='text-align: center; color: red;'>Ошибка загрузки данных: ${error.message}</td></tr>`;
    }
  }
}

// Инициализация UI
const initialLimit = String(state.limit); // Убеждаемся что это строка
if (initialLimit === "custom") {
  customLimitInput.style.display = "inline-block";
  customLimitInput.value = localStorage.getItem("arbitrage_customLimit") || "20";
} else {
  customLimitInput.style.display = "none";
}
limitSelect.value = initialLimit;

renderBlacklist();
renderWhitelist();
fetchInitial();

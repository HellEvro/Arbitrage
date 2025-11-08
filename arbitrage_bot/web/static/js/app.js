const socket = io();
const tableBody = document.querySelector("#ranking-table tbody");

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
  // For very small prices, use more decimal places
  if (price < 0.0001) {
    return price.toFixed(8).replace(/\.?0+$/, ""); // Remove trailing zeros
  } else if (price < 0.01) {
    return price.toFixed(6).replace(/\.?0+$/, "");
  } else if (price < 1) {
    return price.toFixed(4).replace(/\.?0+$/, "");
  } else {
    return price.toFixed(4);
  }
}

function renderOpportunities(opportunities) {
  console.log("Rendering opportunities:", opportunities?.length || 0);
  if (!Array.isArray(opportunities) || opportunities.length === 0) {
    console.warn("No opportunities to render:", opportunities);
    if (tableBody) {
      tableBody.innerHTML = "<tr><td colspan='10' style='text-align: center;'>Нет данных</td></tr>";
    }
    return;
  }
  if (!tableBody) {
    console.error("Table body not found!");
    return;
  }
  tableBody.innerHTML = opportunities
    .map((opp) => {
      return `
        <tr>
          <td><strong>${opp.symbol}</strong></td>
          <td>${createExchangeLink(opp.buy_exchange, opp.buy_symbol || opp.symbol)}</td>
          <td>${formatPrice(opp.buy_price)}</td>
          <td><span class="fee-badge">${opp.buy_fee_pct?.toFixed(3) || "0.100"}%</span></td>
          <td>${createExchangeLink(opp.sell_exchange, opp.sell_symbol || opp.symbol)}</td>
          <td>${formatPrice(opp.sell_price)}</td>
          <td><span class="fee-badge">${opp.sell_fee_pct?.toFixed(3) || "0.100"}%</span></td>
          <td><strong class="profit">${opp.spread_usdt.toFixed(2)}</strong></td>
          <td>${opp.spread_pct.toFixed(3)}%</td>
          <td>${new Date(opp.timestamp_ms).toLocaleTimeString()}</td>
        </tr>
      `;
    })
    .join("");
}

socket.on("opportunities", (data) => {
  console.log("Received opportunities via WebSocket:", data?.length || 0);
  renderOpportunities(data);
});

async function fetchInitial() {
  try {
    console.log("Fetching initial data from /api/ranking");
    const response = await fetch("/api/ranking");
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    console.log("Initial data received:", data?.length || 0, "opportunities");
    renderOpportunities(data);
  } catch (error) {
    console.error("Failed to load initial data", error);
    if (tableBody) {
      tableBody.innerHTML = `<tr><td colspan='10' style='text-align: center; color: red;'>Ошибка загрузки данных: ${error.message}</td></tr>`;
    }
  }
}

fetchInitial();


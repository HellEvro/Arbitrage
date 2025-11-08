const socket = io();
const tableBody = document.querySelector("#ranking-table tbody");

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

function renderOpportunities(opportunities) {
  if (!Array.isArray(opportunities)) return;
  tableBody.innerHTML = opportunities
    .map((opp) => {
      return `
        <tr>
          <td>${opp.symbol}</td>
          <td>${createExchangeLink(opp.buy_exchange, opp.buy_symbol || opp.symbol)}</td>
          <td>${opp.buy_price.toFixed(4)}</td>
          <td>${createExchangeLink(opp.sell_exchange, opp.sell_symbol || opp.symbol)}</td>
          <td>${opp.sell_price.toFixed(4)}</td>
          <td>${opp.spread_usdt.toFixed(2)}</td>
          <td>${opp.spread_pct.toFixed(3)}</td>
          <td>${opp.timestamp_ms}</td>
        </tr>
      `;
    })
    .join("");
}

socket.on("opportunities", (data) => {
  renderOpportunities(data);
});

async function fetchInitial() {
  try {
    const response = await fetch("/api/ranking");
    const data = await response.json();
    renderOpportunities(data);
  } catch (error) {
    console.error("Failed to load initial data", error);
  }
}

fetchInitial();


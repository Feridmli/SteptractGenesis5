import { Buffer } from "buffer";
window.Buffer = window.Buffer || Buffer;

import { ethers } from "ethers";
import { Seaport } from "@opensea/seaport-js";

// ==========================================
// SABİTLƏR (CONSTANTS)
// ==========================================

const ItemType = { NATIVE: 0, ERC20: 1, ERC721: 2, ERC1155: 3 };
const OrderType = { FULL_OPEN: 0, PARTIAL_OPEN: 1, FULL_RESTRICTED: 2, PARTIAL_RESTRICTED: 3 };

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || "https://steptractgenesis.onrender.com";
const NFT_CONTRACT_ADDRESS = import.meta.env.VITE_NFT_CONTRACT || "0xf62049dd99d8a1fa57a31ce091282b2628acc301";

// Seaport Address
const SEAPORT_ADDRESS = "0x0000000000000068f116a894984e2db1123eb395"; 

const ZERO_BYTES32 = "0x0000000000000000000000000000000000000000000000000000000000000000";
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

const APECHAIN_ID = 33139;
const APECHAIN_ID_HEX = "0x8173";

// IPFS
const FALLBACK_IMAGE_URL = "https://ipfs.io/ipfs/QmYtCsNZoe8JurNmM9Gx38uSZdFAWycCpQG31xJ7BnzrTa";

// Global Variables
let provider = null;
let signer = null;
let seaport = null;
let userAddress = null;

let selectedTokens = new Set();
let allNFTs = []; // Lokal verilənlər bazamız

// UI Elements
const connectBtn = document.getElementById("connectBtn");
const disconnectBtn = document.getElementById("disconnectBtn");
const addrSpan = document.getElementById("addr");
const marketplaceDiv = document.getElementById("marketplace");
const noticeDiv = document.getElementById("notice");
const bulkBar = document.getElementById("bulkBar");
const bulkCount = document.getElementById("bulkCount");
const bulkPriceInp = document.getElementById("bulkPrice");
const bulkListBtn = document.getElementById("bulkListBtn");
const searchInput = document.getElementById("searchInput");
const totalVolEl = document.getElementById("totalVol");
const dayVolEl = document.getElementById("dayVol");
const itemsCountEl = document.getElementById("itemsCount");

// ==========================================
// KÖMƏKÇİ FUNKSİYALAR
// ==========================================

function notify(msg, timeout = 3000) {
  if (!noticeDiv) return;
  noticeDiv.textContent = msg;
  if (timeout) setTimeout(() => { if (noticeDiv.textContent === msg) noticeDiv.textContent = ""; }, timeout);
}

function resolveIPFS(url) {
  if (!url) return FALLBACK_IMAGE_URL;
  const GATEWAY = "https://ipfs.io/ipfs/";
  let originalUrl = url;
  if (originalUrl.startsWith("ipfs://")) {
    originalUrl = originalUrl.replace("ipfs://", GATEWAY);
  } else if (originalUrl.startsWith("Qm") && !originalUrl.startsWith("http")) {
    originalUrl = `${GATEWAY}${originalUrl}`;
  }
  return `https://wsrv.nl/?url=${encodeURIComponent(originalUrl)}&w=500&q=75&output=webp&il`;
}

function cleanOrder(orderData) {
  try {
    const order = orderData.order || orderData;
    const { parameters, signature } = order;
    if (!parameters) return null;
    const toStr = (val) => {
        if (val === undefined || val === null) return "0";
        if (typeof val === "object" && val.hex) return BigInt(val.hex).toString();
        return val.toString();
    };
    return {
      parameters: {
        offerer: parameters.offerer,
        zone: parameters.zone,
        offer: parameters.offer.map(item => ({
          itemType: Number(item.itemType), token: item.token,
          identifierOrCriteria: toStr(item.identifierOrCriteria || item.identifier),
          startAmount: toStr(item.startAmount), endAmount: toStr(item.endAmount)
        })),
        consideration: parameters.consideration.map(item => ({
          itemType: Number(item.itemType), token: item.token,
          identifierOrCriteria: toStr(item.identifierOrCriteria || item.identifier),
          startAmount: toStr(item.startAmount), endAmount: toStr(item.endAmount), recipient: item.recipient
        })),
        orderType: Number(parameters.orderType), startTime: toStr(parameters.startTime),
        endTime: toStr(parameters.endTime), zoneHash: parameters.zoneHash,
        salt: toStr(parameters.salt), conduitKey: parameters.conduitKey,
        counter: toStr(parameters.counter),
        totalOriginalConsiderationItems: Number(parameters.totalOriginalConsiderationItems || parameters.consideration.length)
      }, signature: signature
    };
  } catch (e) { return null; }
}

function orderToJsonSafe(obj) {
  return JSON.parse(JSON.stringify(obj, (k, v) => {
    if (v && typeof v === "object") {
      if (ethers.BigNumber.isBigNumber(v)) return v.toString();
      if (v._hex) return ethers.BigNumber.from(v._hex).toString();
    }
    return v;
  }));
}

// ==========================================
// CÜZDAN QOŞULMASI (YENİLƏNMİŞ - RELOADSIZ)
// ==========================================

// Çıxış funksiyası
function handleDisconnect() {
  provider = null;
  signer = null;
  seaport = null;
  userAddress = null;

  connectBtn.style.display = "inline-block";
  disconnectBtn.style.display = "none";
  addrSpan.textContent = "";
  
  // UI-ı yenilə (Buttonlar "Buy"-a çevrilir, "List" yox olur)
  renderNFTs(allNFTs); 
  notify("Çıxış edildi");
}

// Hesab dəyişəndə işləyən funksiya (Reload etmədən)
async function handleAccountsChanged(accounts) {
  if (accounts.length === 0) {
    // Əgər istifadəçi MetaMask-dan tamamilə çıxış edibsə
    handleDisconnect();
  } else {
    // Əgər istifadəçi başqa hesaba keçibsə
    userAddress = accounts[0].toLowerCase();
    
    // Signer və Seaport-u təzə hesab üçün yeniləyirik
    if (provider) {
        signer = provider.getSigner();
        seaport = new Seaport(signer, { 
            overrides: { contractAddress: SEAPORT_ADDRESS, defaultConduitKey: ZERO_BYTES32 } 
        });
    }

    addrSpan.textContent = `Wallet: ${userAddress.slice(0, 6)}...${userAddress.slice(-4)}`;
    notify("Hesab dəyişildi!");
    
    // Düymələri yeniləyirik (Buttonlar sahibə görə dəyişsin)
    connectBtn.style.display = "none";
    disconnectBtn.style.display = "inline-block";
    renderNFTs(allNFTs);
  }
}

async function connectWallet() {
  try {
    if (!window.ethereum) return alert("Metamask tapılmadı!");
    
    provider = new ethers.providers.Web3Provider(window.ethereum, "any");
    
    const { chainId } = await provider.getNetwork();
    if (chainId !== APECHAIN_ID) {
      try {
        await window.ethereum.request({
          method: "wallet_addEthereumChain",
          params: [{
            chainId: APECHAIN_ID_HEX, chainName: "ApeChain Mainnet",
            nativeCurrency: { name: "APE", symbol: "APE", decimals: 18 },
            rpcUrls: [import.meta.env.VITE_APECHAIN_RPC || "https://rpc.apechain.com"],
            blockExplorerUrls: ["https://apescan.io"],
          }],
        });
        provider = new ethers.providers.Web3Provider(window.ethereum, "any");
      } catch (e) { return alert("ApeChain şəbəkəsinə keçilmədi."); }
    }

    const accounts = await provider.send("eth_requestAccounts", []);
    
    // İlk qoşulma məntiqi
    await handleAccountsChanged(accounts);

    // Sign Typed Data fix
    if (signer && !signer.signTypedData) {
        signer.signTypedData = async (domain, types, value) => {
            const typesCopy = { ...types }; delete typesCopy.EIP712Domain; 
            return await signer._signTypedData(domain, typesCopy, value);
        };
    }
    
    notify("Cüzdan qoşuldu!");

    // === RELOAD ƏVƏZİNƏ EVENT LISTENER ===
    window.ethereum.removeListener("accountsChanged", handleAccountsChanged); // Dublikat olmasın deyə əvvəlcə silirik
    window.ethereum.on("accountsChanged", handleAccountsChanged);

  } catch (err) { 
      console.error(err);
      alert("Connect xətası: " + err.message); 
  }
}

disconnectBtn.onclick = handleDisconnect;
connectBtn.onclick = connectWallet;

// ==========================================
// DATA YÜKLƏMƏ
// ==========================================

async function fetchStats() {
    if (!totalVolEl || !dayVolEl) return;
    try {
        const res = await fetch(`${BACKEND_URL}/api/stats`);
        const data = await res.json();
        if(data.success) {
            const fmt = (val) => parseFloat(val).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 });
            totalVolEl.innerText = `${fmt(data.totalVolume)} APE`;
            dayVolEl.innerText = `${fmt(data.dayVolume)} APE`;
        }
    } catch(e) { console.error("Stats Error:", e); }
}

async function loadNFTs() {
  marketplaceDiv.innerHTML = "<p style='color:black; width:100%; text-align:center;'>NFT-lər yüklənir...</p>";
  selectedTokens.clear();
  updateBulkUI();
  fetchStats();

  try {
    const res = await fetch(`${BACKEND_URL}/api/nfts`);
    const data = await res.json();
    allNFTs = data.nfts || [];
    renderNFTs(allNFTs);
  } catch (err) {
    console.error(err);
    marketplaceDiv.innerHTML = "<p style='color:red; text-align:center;'>Yüklənmə xətası.</p>";
  }
}

// ==========================================
// RENDER & HTML GENERATION (YENİ)
// ==========================================

// Tək bir kartı yaradan funksiya (Reloadsız yeniləmə üçün vacibdir)
function createCardElement(nft) {
    const tokenidRaw = (nft.tokenid !== undefined && nft.tokenid !== null) ? nft.tokenid : nft.tokenId;
    if (tokenidRaw === undefined || tokenidRaw === null) return null;
    const tokenid = tokenidRaw.toString(); 

    const name = nft.name || `NFT #${tokenid}`;
    const image = resolveIPFS(nft.image);
    
    let displayPrice = "";
    let priceVal = 0;
    let isListed = false;

    if (nft.price && parseFloat(nft.price) > 0) {
        priceVal = parseFloat(nft.price);
        displayPrice = `${priceVal} APE`;
        isListed = true;
    }

    let canManage = false;
    if (userAddress) {
        if (nft.seller_address && nft.seller_address.toLowerCase() === userAddress) canManage = true; 
        else if (nft.buyer_address && nft.buyer_address.toLowerCase() === userAddress) canManage = true;
    }

    const card = document.createElement("div");
    card.className = "nft-card";
    card.id = `card-${tokenid}`; // Kartı tapmaq üçün ID veririk

    let checkboxHTML = canManage ? `<input type="checkbox" class="select-box" data-id="${tokenid}">` : "";

    let actionsHTML = "";
    if (isListed) {
        if (canManage) {
            actionsHTML = `
                <div style="font-size:12px; color:green; margin-bottom:5px;">Listed: ${displayPrice}</div>
                <input type="number" placeholder="New Price" class="mini-input price-input" step="0.001">
                <button class="action-btn btn-list update-btn">Update</button>
            `;
        } else {
            actionsHTML = `<button class="action-btn btn-buy buy-btn">Buy</button>`;
        }
    } else {
        if (canManage) {
            actionsHTML = `
                <input type="number" placeholder="Price" class="mini-input price-input" step="0.001">
                <button class="action-btn btn-list list-btn">List</button>
            `;
        }
    }

    card.innerHTML = `
        ${checkboxHTML}
        <div class="card-image-wrapper">
            <img src="${image}" loading="lazy" decoding="async" onerror="this.onerror=null; this.src='${FALLBACK_IMAGE_URL}'">
        </div>
        <div class="card-content">
            <div class="card-title">${name}</div>
            <div class="card-details">
                 ${displayPrice && !canManage ? `<div class="price-val">${displayPrice}</div>` : `<div style="height:24px"></div>`}
            </div>
            <div class="card-actions">
                ${actionsHTML}
            </div>
        </div>
    `;

    // Event Listeners
    const chk = card.querySelector(".select-box");
    if (chk) {
        chk.checked = selectedTokens.has(tokenid);
        chk.onchange = (e) => {
            if (e.target.checked) selectedTokens.add(tokenid);
            else selectedTokens.delete(tokenid);
            updateBulkUI();
        };
    }

    // Button Listeners
    if (isListed && !canManage) {
        const btn = card.querySelector(".buy-btn");
        if(btn) btn.onclick = async () => await buyNFT(nft);
    } else {
        const btn = card.querySelector(".list-btn") || card.querySelector(".update-btn");
        if(btn) btn.onclick = async () => {
            const priceInput = card.querySelector(".price-input");
            let inp = priceInput.value;
            if(inp) inp = inp.trim();
            if(!inp || isNaN(inp) || parseFloat(inp) <= 0) return notify("Düzgün qiymət yazın!");
            await listNFT(tokenid, inp);
        };
    }

    return card;
}

function renderNFTs(list) {
    marketplaceDiv.innerHTML = "";
    if (itemsCountEl) itemsCountEl.innerText = list.length;

    if (list.length === 0) {
        marketplaceDiv.innerHTML = "<p style='color:black; width:100%; text-align:center;'>NFT tapılmadı.</p>";
        return;
    }

    list.forEach(nft => {
        const cardElement = createCardElement(nft);
        if(cardElement) marketplaceDiv.appendChild(cardElement);
    });
}

// Səhifəni yeniləmədən tək bir kartı yeniləmək üçün funksiya
function refreshSingleCard(tokenid) {
    const nftData = allNFTs.find(n => n.tokenid == tokenid);
    if (!nftData) return;

    const oldCard = document.getElementById(`card-${tokenid}`);
    const newCard = createCardElement(nftData);

    if (oldCard && newCard) {
        oldCard.replaceWith(newCard); // Köhnə kartı yenisi ilə əvəz edirik (Reloadsız)
    } else if (!oldCard && newCard) {
        marketplaceDiv.appendChild(newCard); 
    }
}

// ==========================================
// SEARCH LISTENER
// ==========================================
if (searchInput) {
    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase();
        const filtered = allNFTs.filter(nft => {
            const name = (nft.name || "").toLowerCase();
            const tid = (nft.tokenid ?? nft.tokenId).toString();
            return name.includes(query) || tid.includes(query);
        });
        renderNFTs(filtered);
    });
}

// ==========================================
// TOPLU UI & LISTING
// ==========================================

function updateBulkUI() {
    if (selectedTokens.size > 0) {
        bulkBar.classList.add("active");
        bulkCount.textContent = `${selectedTokens.size} NFT seçildi`;
    } else {
        bulkBar.classList.remove("active");
    }
}

window.cancelBulk = () => {
    selectedTokens.clear();
    document.querySelectorAll(".select-box").forEach(b => b.checked = false);
    updateBulkUI();
};

if(bulkListBtn) {
    bulkListBtn.onclick = async () => {
        let priceVal = bulkPriceInp.value;
        if(priceVal) priceVal = priceVal.trim();
        if (!priceVal || isNaN(priceVal) || parseFloat(priceVal) <= 0) return alert("Qiymət yazın.");
        await bulkListNFTs(Array.from(selectedTokens), priceVal);
    };
}

async function listNFT(tokenid, priceInEth) {
  if (tokenid === undefined || tokenid === null) return alert("Token ID xətası.");
  await bulkListNFTs([tokenid], priceInEth);
}

async function bulkListNFTs(tokenIds, priceInEth) {
    if (!signer || !seaport) return alert("Cüzdan qoşulmayıb!");
    
    let priceWeiString;
    try {
        priceWeiString = ethers.utils.parseEther(String(priceInEth).trim()).toString();
    } catch (e) { return alert(`Qiymət xətası: ${e.message}`); }

    const cleanTokenIds = tokenIds.map(t => String(t));
    const seller = await signer.getAddress();

    // Approve
    try {
        const nftContract = new ethers.Contract(NFT_CONTRACT_ADDRESS, 
            ["function isApprovedForAll(address,address) view returns(bool)", "function setApprovalForAll(address,bool)"], signer);
        
        if (!(await nftContract.isApprovedForAll(seller, SEAPORT_ADDRESS))) {
            notify("Satış kontraktı təsdiq olunur...");
            const tx = await nftContract.setApprovalForAll(SEAPORT_ADDRESS, true);
            await tx.wait();
            notify("Təsdiqləndi!");
        }
    } catch (e) { return alert("Approve xətası: " + e.message); }

    notify(`${cleanTokenIds.length} NFT orderi imzalanır...`);

    try {
        const startTimeVal = Math.floor(Date.now()/1000).toString(); 
        const endTimeVal = (Math.floor(Date.now()/1000) + 15552000).toString(); 

        const orderInputs = cleanTokenIds.map(tokenStr => {
            return {
                orderType: OrderType.FULL_OPEN, zone: ZERO_ADDRESS, zoneHash: ZERO_BYTES32, conduitKey: ZERO_BYTES32, 
                offer: [{ itemType: ItemType.ERC721, token: NFT_CONTRACT_ADDRESS, identifier: tokenStr, amount: "1" }],
                consideration: [{ itemType: ItemType.NATIVE, token: ZERO_ADDRESS, identifier: "0", amount: priceWeiString, recipient: seller }],
                startTime: startTimeVal, endTime: endTimeVal,
            };
        });

        notify("Zəhmət olmasa cüzdanda imzalayın...");
        const { executeAllActions } = await seaport.createBulkOrders(orderInputs, seller);
        const signedOrders = await executeAllActions(); 

        notify("İmza alındı! UI yenilənir...");

        // ====================================================
        // RELOAD YERİNƏ LOCAL UPDATE (Səhifəni yeniləmirik)
        // ====================================================
        for (const order of signedOrders) {
            const offerItem = order.parameters.offer[0];
            const tokenStr = offerItem.identifierOrCriteria;

            // 1. Bazaya göndəririk (Arxa planda)
            fetch(`${BACKEND_URL}/api/order`, {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    tokenid: tokenStr,
                    price: String(priceInEth),
                    seller_address: seller,
                    seaport_order: orderToJsonSafe(order),
                    order_hash: seaport.getOrderHash(order.parameters),
                    status: "active"
                }),
            });

            // 2. Lokal "allNFTs" massivini yeniləyirik
            const nftIndex = allNFTs.findIndex(n => n.tokenid == tokenStr);
            if (nftIndex !== -1) {
                allNFTs[nftIndex].price = priceInEth;
                allNFTs[nftIndex].seller_address = seller.toLowerCase();
                allNFTs[nftIndex].seaport_order = orderToJsonSafe(order); 
            }

            // 3. UI-da yalnız bu kartı yeniləyirik
            refreshSingleCard(tokenStr);
        }

        notify("Uğurla listələndi!");

    } catch (err) {
        console.error("List Error:", err);
        alert("Satış xətası: " + (err.message || err));
    }
}

// ==========================================
// BUY FUNCTION
// ==========================================

async function buyNFT(nftRecord) {
    if (!signer || !seaport) return alert("Cüzdan qoşulmayıb!");
    
    try {
        const buyerAddress = await signer.getAddress();
        
        if (nftRecord.seller_address && nftRecord.seller_address.toLowerCase() === buyerAddress.toLowerCase()) {
             return alert("Bu NFT artıq sizindir (Satışdasınız).");
        }

        notify("Order hazırlanır...");
        let rawJson = nftRecord.seaport_order;
        if (!rawJson) return alert("Order tapılmadı.");
        if (typeof rawJson === "string") { try { rawJson = JSON.parse(rawJson); } catch (e) {} }

        const cleanOrd = cleanOrder(rawJson);
        if (!cleanOrd) return alert("Order strukturu xətalıdır");

        const { actions } = await seaport.fulfillOrder({ 
            order: cleanOrd, accountAddress: buyerAddress, conduitKey: ZERO_BYTES32 
        });

        const txRequest = await actions[0].transactionMethods.buildTransaction();

        let finalValue = ethers.BigNumber.from(0);
        if (cleanOrd.parameters.consideration) {
            cleanOrd.parameters.consideration.forEach(c => {
                if (Number(c.itemType) === 0) finalValue = finalValue.add(ethers.BigNumber.from(c.startAmount));
            });
        }
        if (txRequest.value && ethers.BigNumber.from(txRequest.value).gt(finalValue)) finalValue = ethers.BigNumber.from(txRequest.value);

        notify("Metamask-da təsdiqləyin...");
        const tx = await signer.sendTransaction({
            to: txRequest.to, data: txRequest.data, value: finalValue, gasLimit: 500000 
        });

        notify("Blokçeyndə təsdiqlənir...");
        await tx.wait();
        
        // ====================================================
        // RELOAD YERİNƏ LOCAL UPDATE (Səhifəni yeniləmirik)
        // ====================================================
        
        // 1. Bazaya məlumat göndəririk
        fetch(`${BACKEND_URL}/api/buy`, {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                tokenid: nftRecord.tokenid, 
                order_hash: nftRecord.order_hash, 
                buyer_address: buyerAddress,
                price: nftRecord.price, 
                seller: nftRecord.seller_address 
            }),
        });
        
        notify("Uğurlu alış! UI yenilənir...");

        // 2. Lokal "allNFTs" massivini yeniləyirik
        const nftIndex = allNFTs.findIndex(n => n.tokenid == nftRecord.tokenid);
        if (nftIndex !== -1) {
            allNFTs[nftIndex].price = 0; // Satışdan çıxdı
            allNFTs[nftIndex].seller_address = null;
            allNFTs[nftIndex].buyer_address = buyerAddress.toLowerCase();
            allNFTs[nftIndex].seaport_order = null;
        }

        // 3. UI-da yalnız bu kartı yeniləyirik
        refreshSingleCard(nftRecord.tokenid);

        // 4. Statistikanı (Volume) yeniləyirik
        fetchStats();

    } catch (err) {
        console.error("Buy Error:", err);
        alert("Buy Xətası: " + (err.message || "Bilinməyən xəta"));
    }
}

// Başlanğıc Yükləmə
loadNFTs();

window.loadNFTs = loadNFTs;

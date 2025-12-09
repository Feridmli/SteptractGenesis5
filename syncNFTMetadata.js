import { ethers } from "ethers";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import fetch from "node-fetch";

dotenv.config();

// =======================================
// SUPABASE SETUP
// =======================================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// =======================================
// CONSTANTS
// =======================================
const NFT_CONTRACT_ADDRESS = process.env.NFT_CONTRACT_ADDRESS;
const MARKETPLACE_CONTRACT_ADDRESS = process.env.SEAPORT_CONTRACT_ADDRESS;

// RPC List - ApeChain
const RPC_LIST = [
  process.env.APECHAIN_RPC,
  "https://rpc.apechain.com/http",
  "https://apechain.drpc.org",
  "https://33139.rpc.thirdweb.com"
];

// IPFS Gateways (Sürət üçün bir neçəsini yoxlayacağıq)
const IPFS_GATEWAYS = [
  "https://dweb.link/ipfs/",
  "https://ipfs.io/ipfs/",
  "https://gateway.pinata.cloud/ipfs/",
  "https://cloudflare-ipfs.com/ipfs/"
];

let providerIndex = 0;
function getProvider() {
  const rpc = RPC_LIST[providerIndex % RPC_LIST.length];
  if(!rpc) return new ethers.providers.JsonRpcProvider("https://rpc.apechain.com/http");
  providerIndex++;
  return new ethers.providers.JsonRpcProvider(rpc);
}

let provider = getProvider();

// =======================================
// NFT ABI
// =======================================
const nftABI = [
  "function ownerOf(uint256 tokenid) view returns (address)",
  "function totalSupply() view returns (uint256)",
  "function tokenURI(uint256 tokenid) view returns (string)"
];

let nftContract = new ethers.Contract(NFT_CONTRACT_ADDRESS, nftABI, provider);

// =======================================
// HELPERS
// =======================================

// IPFS linkini təmizləyən funksiya
function resolveLink(uri, gateway = "https://ipfs.io/ipfs/") {
  if (!uri) return null;
  if (uri.startsWith("ipfs://")) {
    return uri.replace("ipfs://", gateway);
  }
  return uri;
}

// Metadata çəkən gücləndirilmiş funksiya
async function fetchMetadataWithRetry(tokenURI) {
  // Əgər tokenURI artıq http linkdirsə və ipfs deyilsə, birbaşa yoxla
  if (tokenURI.startsWith("http") && !tokenURI.includes("ipfs")) {
     try {
         const res = await fetch(tokenURI, { timeout: 5000 });
         if (res.ok) return await res.json();
     } catch(e) {}
  }

  // IPFS hash-i çıxarırıq
  let ipfsHash = tokenURI;
  if (tokenURI.startsWith("ipfs://")) {
    ipfsHash = tokenURI.replace("ipfs://", "");
  } else if (tokenURI.includes("/ipfs/")) {
    ipfsHash = tokenURI.split("/ipfs/")[1];
  }

  // Müxtəlif gateway-lərlə yoxlayırıq
  for (const gateway of IPFS_GATEWAYS) {
    try {
      const url = `${gateway}${ipfsHash}`;
      // 5 saniyə timeout qoyuruq
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);
      
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timeoutId);

      if (res.ok) {
        const data = await res.json();
        return data; // Uğurlu
      }
    } catch (err) {
      // Bu gateway işləmədi, növbətiyə keç
      continue;
    }
  }
  throw new Error("Metadata fetch failed from all gateways");
}

// =======================================================
//   PROCESS NFT
// =======================================================
async function processNFT(tokenid) {
  try {
    let owner, tokenURI, success = false;

    // 1. Blockchain-dən Owner və URI götürürük
    for (let i = 0; i < RPC_LIST.length; i++) {
      try {
        owner = await nftContract.ownerOf(tokenid);
        tokenURI = await nftContract.tokenURI(tokenid);
        success = true;
        break;
      } catch (err) {
        if (err.message?.includes("nonexistent token")) {
            console.warn(`⚠️ Token #${tokenid} mövcud deyil.`);
            return;
        }
        provider = getProvider();
        nftContract = new ethers.Contract(NFT_CONTRACT_ADDRESS, nftABI, provider);
      }
    }

    if (!success) throw new Error("RPC failed");

    // 2. Metadata yükləyirik
    let name = `NFT #${tokenid}`;
    let image = null;

    try {
      const metadata = await fetchMetadataWithRetry(tokenURI);
      
      if (metadata) {
          if (metadata.name) name = metadata.name;
          if (metadata.image) image = metadata.image;
          else if (metadata.image_url) image = metadata.image_url; // Bəzi standartlarda belə olur
      }
    } catch (e) {
      console.log(`⚠️ Metadata error for #${tokenid}:`, e.message);
      // Xəta olsa, image NULL qalır (JSON linki olmur!), ad isə NFT #ID qalır.
    }

    // 3. Bazanı yoxla (Satış statusu üçün)
    const { data: existingData } = await supabase
      .from("metadata")
      .select("buyer_address, seaport_order, price, order_hash")
      .eq("tokenid", tokenid.toString())
      .single();

    let shouldWipeOrder = false;
    if (existingData && existingData.buyer_address && existingData.buyer_address.toLowerCase() !== owner.toLowerCase()) {
      shouldWipeOrder = true; // Sahibi dəyişib, listinqi sil
    }

    // 4. Məlumatları hazırla
    const upsertData = {
      tokenid: tokenid.toString(),
      nft_contract: NFT_CONTRACT_ADDRESS,
      marketplace_contract: MARKETPLACE_CONTRACT_ADDRESS,
      buyer_address: owner.toLowerCase(),
      on_chain: true,
      name: name,
      image: image, // Artıq JSON linki yox, təmiz IPFS linki və ya null olacaq
      updatedat: new Date().toISOString()
    };

    if (!shouldWipeOrder && existingData) {
      upsertData.seaport_order = existingData.seaport_order;
      upsertData.price = existingData.price;
      upsertData.order_hash = existingData.order_hash;
    } else {
      upsertData.seaport_order = null;
      upsertData.price = null;
      upsertData.order_hash = null;
    }

    // 5. Bazaya yaz
    const { error } = await supabase.from("metadata").upsert(upsertData, { onConflict: "tokenid" });

    if(error) console.error(`DB Error #${tokenid}:`, error.message);
    else console.log(`✅ Synced #${tokenid}: ${name}`);

  } catch (e) {
    console.warn(`❌ Fail #${tokenid}:`, e.message);
  }
}

// =======================================================
// MAIN LOOP
// =======================================================
async function main() {
  try {
    const totalSupply = await nftContract.totalSupply();
    console.log(`🚀 Total Supply: ${totalSupply}`);

    const BATCH_SIZE = 10; // Batch-i azaltdıq ki, fetch xətaları azalsın
    
    for (let i = 1; i <= totalSupply; i += BATCH_SIZE) {
      const batch = [];
      for(let j=0; j<BATCH_SIZE; j++) {
          if(i+j <= totalSupply) batch.push(i+j);
      }
      
      // Paralel işləmə sürəti
      await Promise.all(batch.map(id => processNFT(id)));
    }

    console.log("🎉 Sync tamamlandı!");
  } catch (err) {
    console.error("Fatal:", err);
  }
}

main();

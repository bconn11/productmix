(async function () {
  const qs = new URLSearchParams(location.search);
  const shop = qs.get("shop") || "";
  document.getElementById("shop-pill").textContent = shop || "(add ?shop=…)";

  const startEl = document.getElementById("start");
  const endEl   = document.getElementById("end");
  const groupEl = document.getElementById("groupBy");
  const metricEl= document.getElementById("metric");
  const chartEl = document.getElementById("chartType");
  const showOrdersEl = document.getElementById("showOrders");
  const btnLoad = document.getElementById("btnLoadSales");
  const statusEl = document.getElementById("status");

  const filtersCard = document.getElementById("typeFilters");
  const chipsWrap   = document.getElementById("typeChips");
  const btnAll      = document.getElementById("btnAll");
  const btnNone     = document.getElementById("btnNone");

  // default last 14 days (just calendar strings; server does timezone-aware work)
  const today = new Date();
  const endDef = new Date(Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())).toISOString().slice(0,10);
  const startDef = new Date(Date.UTC(today.getFullYear(), today.getMonth(), today.getDate() - 13)).toISOString().slice(0,10);
  startEl.value = startDef; endEl.value = endDef;

  let rows = [];
  let seriesKeys = [];
  let selected = new Set();
  let currency = null;

  function setStatus(msg, err=false){ statusEl.textContent = msg; statusEl.style.color = err ? "#b91c1c" : "#6b7280"; }

  async function jget(url){
    const r = await fetch(url, {credentials:"same-origin"});
    if(!r.ok){ throw new Error(`HTTP ${r.status}: ${await r.text()}`); }
    return r.json();
  }

  function buildTypeFilters(keys){
    chipsWrap.innerHTML = "";
    selected = new Set(keys);
    keys.forEach((k,i)=>{
      const id = `pt_${i}`;
      const label = document.createElement("label");
      label.className = "chip";
      const cb = document.createElement("input");
      cb.type="checkbox"; cb.id=id; cb.checked=true; cb.dataset.key=k;
      cb.addEventListener("change", render);
      const span = document.createElement("span"); span.textContent = k;
      label.appendChild(cb); label.appendChild(span);
      chipsWrap.appendChild(label);
    });
    btnAll.onclick = ()=>{ document.querySelectorAll("#typeChips input").forEach(cb=>cb.checked=true); selected = new Set(keys); render(); };
    btnNone.onclick= ()=>{ document.querySelectorAll("#typeChips input").forEach(cb=>cb.checked=false); selected = new Set(); render(); };
  }
  function refreshSelected(){
    const keys=[]; document.querySelectorAll("#typeChips input:checked").forEach(cb=>keys.push(cb.dataset.key));
    selected = new Set(keys);
  }

  function render(){
    refreshSelected();
    const metric = metricEl.value;
    const chart = chartEl.value;
    const dates = rows.map(r=>r.date);
    const keys = seriesKeys.filter(k=>selected.has(k));
    const traces = [];

    if(chart==="bar"){
      for(const k of keys){
        traces.push({ x: dates, y: rows.map(r=>r[k]||0), type:"bar", name:k, hovertemplate:`%{x}<br>${k}: %{y}<extra></extra>` });
      }
    }else{
      for(const k of keys){
        traces.push({ x: dates, y: rows.map(r=>r[k]||0), type:"scatter", mode:"lines+markers", name:k,
          hovertemplate:`%{x}<br>${k}: %{y}<extra></extra>` });
      }
    }

    if(showOrdersEl.checked){
      traces.push({ x: dates, y: rows.map(r=>r.orders_total||0), type:"scatter", mode:"lines+markers", name:"Orders", yaxis:"y2" });
    }

    const layout = {
      title: `Daily Product Mix (${metric==="sales" ? "Sales $" : "Units"})`,
      barmode: "stack",
      xaxis: { title:"Date", type:"category" },
      yaxis: { title: metric==="sales" ? `Sales (${currency||"USD"})` : "Units" },
      yaxis2:{ title:"Orders", overlaying:"y", side:"right", showgrid:false },
      margin:{t:40,r:60,b:60,l:60}
    };
    Plotly.newPlot("plot", traces, layout, {displayModeBar:true,responsive:true});
  }

  async function loadSales(){
    try{
      setStatus("Loading …");
      const s = startEl.value; const e = endEl.value;
      const gb = groupEl.value; const mt = metricEl.value;
      const url = `/api/sales?shop=${encodeURIComponent(shop)}&start_date=${encodeURIComponent(s)}&end_date=${encodeURIComponent(e)}&group_by=${encodeURIComponent(gb)}&metric=${encodeURIComponent(mt)}`;
      const res = await jget(url);
      rows = res.rows || []; currency = res.currency || null;

      const setKeys = new Set();
      rows.forEach(r=>{
        Object.keys(r).forEach(k=>{
          if(k!=="date" && k!=="total" && k!=="orders_total" && k!=="units_total" && k!=="sales_total") setKeys.add(k);
        });
      });
      seriesKeys = Array.from(setKeys).sort();

      if(gb==="product_type"){ document.getElementById("typeFilters").classList.remove("hide"); buildTypeFilters(seriesKeys); }
      else { document.getElementById("typeFilters").classList.add("hide"); selected = new Set(seriesKeys); }

      setStatus(`Loaded ${rows.length} days • series=${seriesKeys.length} • tz=${res.tz || "?"}`);
      render();
    }catch(e){
      console.error(e); setStatus(`Load failed: ${e.message}`, true);
    }
  }

  document.getElementById("btnLoadSales").addEventListener("click", loadSales);
  ["change"].forEach(ev=>{
    document.getElementById("chartType").addEventListener(ev, render);
    document.getElementById("metric").addEventListener(ev, loadSales);
    document.getElementById("groupBy").addEventListener(ev, loadSales);
    document.getElementById("showOrders").addEventListener(ev, render);
    document.getElementById("start").addEventListener(ev, loadSales);
    document.getElementById("end").addEventListener(ev, loadSales);
  });

  if(shop){ loadSales(); } else { setStatus("Add ?shop=your-store.myshopify.com", true); }
})();

let isFetching = false;
let currentQuery = "";
let captchaTimer = null;
const seenVideoIds = new Set();
let isShortSearchPage = false;
let shortSearchPage = 1;

// 検索エンジンの初期化設定
window.__gcse = {
  parsetags: 'explicit',
  initializationCallback: function() {
    google.search.cse.element.render({
      div: "hidden-cse-container",
      tag: 'searchresults-only',
      gname: 'shortsCse'
    });
    // ホーム画面または検索時でShortsデータが無い場合にCSE経由で初期ロード
    if ($("#home-shorts-container").length > 0 && $("#home-shorts-container").find('.video-card.is-short').length === 0) {
      currentQuery = currentSearchQuery ? currentSearchQuery : "人気 #shorts";
      isShortSearchPage = false;
      triggerCseShortsSearch(currentQuery);
    }
    // Shorts検索ページかどうかを判定
    if ($("#results-container.shorts-grid").length > 0) {
      isShortSearchPage = true;
      shortSearchPage = {{ page if page else 1 }};
    }
  },
  searchCallbacks: {
    web: {
      ready: function(name, q, promos, results) {
        // レスポンス受信時はタイマー解除およびCSEコンテナの非表示設定
        clearTimeout(captchaTimer);
        const cseContainer = document.getElementById('hidden-cse-container');
        if (cseContainer) {
          cseContainer.style.display = 'none';
        }

        isFetching = false;
        let videos = [];
        if (results && results.length > 0) {
          results.forEach(r => {
            const id = extractShortsId(r.unescapedUrl || r.url || "");
            if (id && !seenVideoIds.has(id)) {
              seenVideoIds.add(id);
              videos.push({ id: id, title: r.titleNoFormatting || r.title });
            }
          });
        }
        
        // Shorts検索ページでの描画
        if (isShortSearchPage && $("#results-container.shorts-grid").length > 0) {
          renderShortsPageResults(videos);
        } else {
          // ホーム画面での描画
          renderCseShortsResults(videos);
        }
        return true;
      }
    }
  }
};

// URLからショート動画の11桁IDのみを抽出するロジック
function extractShortsId(url) {
  const match = url.match(/(?:shorts\/|v=|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
  return match ? match[1] : null;
}

// 検索の実行ロジック (appendフラグで新規検索か追加検索かを判別)
function triggerCseShortsSearch(query, append = false) {
  if (isFetching) return;
  isFetching = true;
  
  if (!append) {
    seenVideoIds.clear();
  }

  // 既存のタイマーがあればクリア
  clearTimeout(captchaTimer);
  
  // 3秒経過してもレスポンスがなくisFetchingがtrueならCAPTCHA（ロボット確認）を表示
  captchaTimer = setTimeout(() => {
    if (isFetching) {
      const cseContainer = document.getElementById('hidden-cse-container');
      cseContainer.style.display = 'block';
      cseContainer.style.position = 'fixed';
      cseContainer.style.top = '100px';
      cseContainer.style.left = '50%';
      cseContainer.style.transform = 'translateX(-50%)';
      cseContainer.style.zIndex = '10000';
      alert("ロボット確認が必要です。画面に表示される指示に従ってください。完了すると自動で画面に戻ります。");
    }
  }, 3000);

  const element = google.search.cse.element.getElement('shortsCse');
  
  // 追加読み込み時はランダムな装飾キーワードを付けて検索リクエストを変化させる
  const decor = append ? ["", " viral", " trend", " popular", " short", " new"][Math.floor(Math.random() * 6)] : "";
  
  if (element) {
    element.execute(query + decor + " site:youtube.com/shorts");
  } else {
    isFetching = false;
  }
}

// Shorts検索ページでの無限スクロール対応ロード
function loadNextShortsPage() {
  if (isFetching) return;
  isFetching = true;
  
  // 読み込み中インジケータを表示
  $("#infinite-loader").css("display", "flex");

  const nextPage = shortSearchPage + 1;
  const query = encodeURIComponent(currentSearchQuery || "");
  const type = "short";

  const url = `/search?q=${query}&type=${type}&page=${nextPage}`;

  $.ajax({
    url: url,
    type: 'GET',
    dataType: 'html',
    success: function (response) {
      const newItems = $(response).find('#results-container').html();
      
      if (newItems && $.trim(newItems).length > 0) {
        // 新しいアイテムを追加
        $('#results-container').append(newItems);
        shortSearchPage = nextPage;
      }
    },
    error: function () {
      showNotify("追加データの読み込みに失敗しました。", true);
    },
    complete: function () {
      isFetching = false;
      $("#infinite-loader").hide();
    }
  });
}

// 画面へのカード描画（ホーム画面用）
function renderCseShortsResults(videos) {
  const container = $('#home-shorts-container');
  
  // 初回描写時にスケルトンカードを消去
  container.find('.skeleton-card').remove();

  videos.forEach(v => {
    const html = `
      <a href="/shorts/${v.id}" class="video-card is-short">
        <div class="thumbnail-box">
          <img class="thumbnail" loading="lazy" src="https://i.ytimg.com/vi/${v.id}/hqdefault.jpg" alt="${v.title}">
        </div>
        <div class="video-info">
          <div class="video-details">
            <div class="video-title">${v.title}</div>
          </div>
        </div>
      </a>
    `;
    container.append(html);
  });
}

function showNotify(msg, isError = false) {
  const alertBox = document.getElementById("custom-alert");
  const alertIcon = document.getElementById("alert-icon");
  const alertText = document.getElementById("alert-text");

  alertText.innerText = msg;

  if (isError) {
    alertIcon.innerText = "warning";
    alertIcon.style.color = "var(--yt-brand-red)";
  } else {
    alertIcon.innerText = "info";
    alertIcon.style.color = "var(--yt-text-primary)";
  }

  $(alertBox).stop(true, true).fadeIn(300).delay(3000).fadeOut(500);
}

// search-shorts-page.htmlでのハンドラー
function handleShortsSearch(e) {
  if (e) {
    e.preventDefault();
  }
  const q = document.getElementById('search-input').value;
  if (!q) return;
  
  currentQuery = q;
  document.getElementById('shorts-grid').innerHTML = '';
  document.getElementById('page-title').innerText = `「${q}」のShorts検索結果`;
  seenVideoIds.clear();
  triggerCseShortsSearch(currentQuery, false);
}

// 無限スクロールの検知イベント
function initInfiniteScroll() {
  window.addEventListener('scroll', () => {
    const scrollPos = window.innerHeight + window.scrollY;
    const bodyHeight = document.body.offsetHeight;
    
    // 画面最下部から500px以内に近づいたら追加読み込みを実行
    if (scrollPos >= bodyHeight - 500 && !isFetching && currentQuery) {
      triggerCseShortsSearch(currentQuery, true);
    }
  });
}

// 汎用の無限スクロール初期化（search.htmlで使用）
function initShortsInfiniteScroll() {
  $(window).on('scroll', function () {
    if (isFetching) return;
    
    const scrollTop = $(window).scrollTop();
    const windowHeight = $(window).height();
    const documentHeight = $(document).height();

    // 画面最下部から300px手前でトリガー
    if (scrollTop + windowHeight >= documentHeight - 300) {
      loadNextShortsPage();
    }
  });
}

// search-shorts-page.htmlでのカード描画（スタンドアロンページ用）
function renderShortsPageResults(videos) {
  const container = document.getElementById('shorts-grid');
  const loaderEl = document.getElementById('loader');
  if (loaderEl) {
    loaderEl.classList.add('hidden');
  }
  
  videos.forEach(v => {
    const card = document.createElement('div');
    card.className = 'shorts-card';
    card.onclick = () => playShorts(v.id);
    card.innerHTML = `
      <div class="thumbnail-container">
        <img src="https://i.ytimg.com/vi/${v.id}/hqdefault.jpg" alt="${v.title}">
      </div>
      <div class="shorts-info">
        <div class="shorts-title">${v.title}</div>
      </div>
    `;
    container.appendChild(card);
  });
}

// search.htmlのShorts検索結果ページでのカード描画
function renderShortsGridResults(videos) {
  const container = $('#results-container');
  
  videos.forEach(v => {
    const html = `
      <a href="/shorts/${v.id}" class="video-card is-short">
        <div class="thumbnail-box">
          <img class="thumbnail" loading="lazy" src="https://i.ytimg.com/vi/${v.id}/hqdefault.jpg" alt="${v.title}">
        </div>
        <div class="video-info">
          <div class="video-details">
            <div class="video-title">${v.title}</div>
          </div>
        </div>
      </a>
    `;
    container.append(html);
  });
}

// モーダルで縦型プレイヤーを起動
function playShorts(id) {
  const modal = document.getElementById('player-modal');
  const player = document.getElementById('yt-player');
  player.src = `https://www.youtube-nocookie.com/embed/${id}?autoplay=1&loop=1&playlist=${id}`;
  modal.classList.add('active');
}

function closePlayer(e) {
  if (e) {
    e.preventDefault();
  }
  const modal = document.getElementById('player-modal');
  const player = document.getElementById('yt-player');
  player.src = '';
  modal.classList.remove('active');
}

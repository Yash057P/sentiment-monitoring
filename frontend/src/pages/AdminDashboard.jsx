import { useEffect, useState } from 'react';
import { TrendingUp, TrendingDown, Minus, ExternalLink, RotateCcw } from 'lucide-react';
import { api, formatTime, resetLiveData } from '../api.js';
import { useT } from '../i18n.jsx';
import Drawer from '../components/Drawer.jsx';
import { TopBar, StatusBadge, SentimentBadge, Kpi, Empty, MiniBar } from '../components/ui.jsx';
import { companies } from '../components/companies.js';

function BuildsSince({ overview }) {
  const { t } = useT();
  const since = overview?.generatedAt;
  return (
    <span className="text-muted" style={{ fontSize: 12 }}>
      {t('overview.updated')} {since ? new Date(since).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '—'}
    </span>
  );
}

export default function AdminDashboard() {
  const { t } = useT();
  const [data, setData] = useState(null);
  const [tweets, setTweets] = useState([]);
  const [companyFilter, setCompanyFilter] = useState('');
  const [sentimentFilter, setSentimentFilter] = useState('');
  const [error, setError] = useState('');
  const [resetting, setResetting] = useState(false);
  const [notice, setNotice] = useState(null);

  async function doReset() {
    if (!window.confirm(t('admin.resetConfirm'))) return;
    setResetting(true);
    setNotice(null);
    try {
      await resetLiveData(true);
      setData(null);
      setTweets([]);
      setCompanyFilter('');
      setSentimentFilter('');
      setNotice({ kind: 'ok', text: t('admin.resetDone') });
    } catch (e) {
      setNotice({ kind: 'err', text: e.message });
    } finally {
      setResetting(false);
      setTimeout(() => setNotice(null), 5000);
    }
  }

  useEffect(() => {
    let alive = true;
    async function tick() {
      try {
        const d = await api('/api/admin/overview');
        if (alive) setData(d);
      } catch (e) {
        if (alive) setError(e.message);
      }
    }
    tick();
    const int = setInterval(tick, 5000);
    return () => {
      alive = false;
      clearInterval(int);
    };
  }, []);

  useEffect(() => {
    let alive = true;
    async function loadTweets() {
      try {
        const q = new URLSearchParams();
        if (companyFilter) q.set('company', companyFilter);
        if (sentimentFilter) q.set('sentiment', sentimentFilter);
        const d = await api(`/api/admin/tweets?${q}`);
        if (alive) setTweets(d.tweets || []);
      } catch {}
    }
    loadTweets();
    const int = setInterval(loadTweets, 5000);
    return () => {
      alive = false;
      clearInterval(int);
    };
  }, [companyFilter, sentimentFilter]);

  if (error && !data) return <Empty title="Cannot reach backend" hint={error} />;

  const trends = data
    ? data.companies.map((c) => [
        c.handle,
        (c.slope || 0) > 0.01 ? 'up' : (c.slope || 0) < -0.01 ? 'down' : 'flat',
      ])
    : [];
  const trendIcon = (t) =>
    t === 'up' ? <TrendingUp size={14} color="var(--good)" /> : t === 'down' ? <TrendingDown size={14} color="var(--bad)" /> : <Minus size={14} color="var(--warn)" />;

  return (
    <div>
      <Drawer />
      <TopBar title="Admin" subtitle={`${data ? data.companies.length : 6} ${t('companies.title').toLowerCase()} · live`} />
      <main className="container">
        {data && (
          <>
            <div className="row" style={{ marginBottom: 16 }}>
              <h1 style={{ fontSize: '1.4rem' }}>{t('overview.platform')}</h1>
              <div className="spacer" />
              <BuildsSince overview={data} />
              <button className="btn ghost sm" onClick={doReset} disabled={resetting}>
                <RotateCcw size={15} /> {resetting ? '…' : t('admin.reset')}
              </button>
              {notice && (
                <span className={`badge ${notice.kind === 'ok' ? 'good' : 'bad'}`}>{notice.text}</span>
              )}
            </div>

            <div className="grid kpis" style={{ marginBottom: 16 }}>
              <Kpi label={t('overview.totaltweets')} value={data.totalTweets} sub={t('overview.allPlatforms')} tone="good" />
              <Kpi label={t('overview.positive')} value={data.positive} sub={t('overview.pct', { v: data.positivePct })} />
              <Kpi label={t('overview.negative')} value={data.negative} sub={t('overview.pct', { v: data.negativePct })} tone="bad" />
              <Kpi label={t('overview.outlook')} value={<StatusBadge outlook={data.outlook} />} sub={data.forecast !== null ? t('overview.next', { v: data.forecast }) : t('overview.building')} tone="good" />
              <Kpi label={t('overview.general')} value={data.generalTweets} sub={t('co.generic')} tone="warn" />
            </div>

            <div className="card" style={{ marginBottom: 16 }}>
              <h3>{t('companies.title')}</h3>
              <p className="card-title">{t('companies.sub')}</p>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>{t('feed.company')}</th>
                      <th>{t('companies.sector')}</th>
                      <th>{t('companies.tweets')}</th>
                      <th>{t('companies.positivity')}</th>
                      <th>{t('companies.outlook')}</th>
                      <th>{t('companies.forecast')}</th>
                      <th>{t('companies.trend')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.companies.map((c) => {
                      const t = (trends.find(([h]) => h === c.handle) || [])[1];
                      return (
                        <tr key={c.handle} className="company-row" onClick={() => setCompanyFilter(c.handle)}>
                          <td>
                            <span className="row" style={{ gap: 10 }}>
                              <span className="logo-cell" style={{ background: c.color }}>{c.logo}</span>
                              <b>{c.name}</b>
                            </span>
                          </td>
                          <td className="text-muted">{c.sector}</td>
                          <td className="mono">{c.tweets}</td>
                          <td>
                            <span className="row" style={{ gap: 10 }}>
                              <MiniBar pct={c.positive_pct / 100} color={c.color} />
                              <span className="mono">{c.positive_pct}%</span>
                            </span>
                          </td>
                          <td><StatusBadge outlook={c.outlook} /></td>
                          <td className="mono">{c.forecast !== null ? `${c.forecast}%` : '—'}</td>
                          <td>{trendIcon(t)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <p className="text-muted" style={{ fontSize: 12, marginTop: 10 }}>
                {t('companies.click')}
              </p>
            </div>
          </>
        )}

        <div className="card">
          <div className="row" style={{ marginBottom: 16 }}>
            <h3>{t('feed.title')}</h3>
            <div className="spacer" />
            <select className="select" value={companyFilter} onChange={(e) => setCompanyFilter(e.target.value)}>
              <option value="">{t('feed.allCompanies')}</option>
              {companies.map((c) => (
                <option key={c.handle} value={c.handle}>{c.name}</option>
              ))}
            </select>
            <select className="select" value={sentimentFilter} onChange={(e) => setSentimentFilter(e.target.value)}>
              <option value="">{t('feed.allSentiment')}</option>
              <option value="positive">{t('overview.positive')}</option>
              <option value="negative">{t('overview.negative')}</option>
            </select>
          </div>
          {tweets.length === 0 ? (
            <Empty title={t('feed.emptyTitle')} hint={t('feed.emptyHint')} />
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('feed.time')}</th>
                    <th>{t('feed.company')}</th>
                    <th>{t('feed.tweet')}</th>
                    <th>{t('feed.prediction')}</th>
                    <th>{t('feed.source')}</th>
                  </tr>
                </thead>
                <tbody>
                  {tweets.map((tweet, i) => (
                    <tr key={`${tweet.tweet_id}-${i}`}>
                      <td className="mono text-muted" style={{ whiteSpace: 'nowrap' }}>{formatTime(tweet.ingestion_ts_ms)}</td>
                      <td style={{ whiteSpace: 'nowrap' }}>
                        {tweet.company ? (
                          <span className="row" style={{ gap: 8 }}>
                            <span className="logo-cell" style={{ background: companies.find((c) => c.handle === tweet.company)?.color || '#64748b', width: 26, height: 26, borderRadius: 8 }}>{companies.find((c) => c.handle === tweet.company)?.logo || '•'}</span>
                            <span>{tweet.company_name}</span>
                          </span>
                        ) : (
                          <span className="text-muted">{t('co.generic')}</span>
                        )}
                      </td>
                      <td className="tweet-text">{tweet.text}</td>
                      <td style={{ whiteSpace: 'nowrap' }}><SentimentBadge sentiment={tweet.predicted_sentiment} /></td>
                      <td className="text-muted">{tweet.source === 'enrichment' ? t('co.brand') : t('co.dataset')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <p className="footer">
          BrandScope · Kafka → Spark Streaming → Spark MLlib → React
          <ExternalLink size={12} style={{ verticalAlign: '-2px', marginLeft: 4 }} />
        </p>
      </main>
    </div>
  );
}
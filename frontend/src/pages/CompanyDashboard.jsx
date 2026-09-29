import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertTriangle, ThumbsUp } from 'lucide-react';
import { api, formatTime } from '../api.js';
import { useT } from '../i18n.jsx';
import { useChartTheme } from '../theme.jsx';
import Drawer from '../components/Drawer.jsx';
import { TopBar, StatusBadge, SentimentBadge, Kpi, Empty } from '../components/ui.jsx';

export default function CompanyDashboard() {
  const { t } = useT();
  const { tick: tickColor, grid: gridColor } = useChartTheme();
  const [data, setData] = useState(null);
  const [tab, setTab] = useState('positive');
  const [error, setError] = useState('');

  useEffect(() => {
    let alive = true;
    async function tick() {
      try {
        const d = await api('/api/company/overview');
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

  const timeline = useMemo(() => (data?.timeline || []).slice(-60), [data]);
  const series = timeline.map((p) => ({
    time: new Date(+p.t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    positive_pct: p.positive_pct,
    total: p.total,
  }));

  if (error && !data) return <Empty title="Cannot reach backend" hint={error} />;

  const outlookColor = data?.outlook === 'good' ? 'var(--good)' : data?.outlook === 'bad' ? 'var(--bad)' : 'var(--warn)';
  const trendIcon =
    (data?.slope || 0) > 0.01 ? (
      <TrendingUp size={16} color="var(--good)" />
    ) : (data?.slope || 0) < -0.01 ? (
      <TrendingDown size={16} color="var(--bad)" />
    ) : (
      <Minus size={16} color="var(--warn)" />
    );
  const pieData = [
    { name: 'Positive', value: data?.positive || 0 },
    { name: 'Negative', value: data?.negative || 0 },
  ];
  const tweets = tab === 'positive' ? data?.positiveTweets || [] : data?.negativeTweets || [];

  return (
    <div>
      <Drawer />
      <TopBar
        title={data?.name}
        companyLogo={data?.logo}
        subtitle={`${data?.sector} · ${data?.tagline || ''}`}
      />
      <main className="container">
        <div className="row" style={{ marginBottom: 16 }}>
          <h1 style={{ fontSize: '1.3rem' }}>{data?.name} {t('co.dashboard')}</h1>
          <div className="spacer" />
          {data && <StatusBadge outlook={data.outlook} />}
        </div>

        {data && (
          <>
            <div className="grid kpis" style={{ marginBottom: 16 }}>
              <Kpi label={t('co.mention')} value={data.tweets} sub={t('co.mixed')} tone="good" />
              <Kpi label={t('co.positive')} value={data.positive} sub={t('co.yourPct', { v: data.positive_pct })} />
              <Kpi label={t('co.negimpact')} value={data.negative} sub={t('co.watch', { v: data.negative_pct })} tone="bad" />
              <Kpi label={t('co.forecast')} value={data.forecast !== null ? `${data.forecast}%` : '—'} sub={trendIcon} />
            </div>

            <div className="grid overview" style={{ marginBottom: 16 }}>
              <div className="card">
                <h3>{t('co.overtime')}</h3>
                <p className="card-title">{t('co.posPerWindow')}</p>
                {series.length >= 2 ? (
                  <ResponsiveContainer width="100%" height={260}>
                    <AreaChart data={series} margin={{ top: 8, right: 8, left: -16, bottom: 0 }}>
                      <defs>
                        <linearGradient id="posGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor={data.color} stopOpacity={0.35} />
                          <stop offset="100%" stopColor={data.color} stopOpacity={0.02} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
                      <XAxis dataKey="time" tick={{ fontSize: 11, fill: tickColor }} tickLine={false} axisLine={false} minTickGap={24} />
                      <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: tickColor }} tickLine={false} axisLine={false} />
                      <Tooltip
                        formatter={(v, n) => (n === 'positive_pct' ? [`${v}% positive`, 'Positivity'] : [v, 'Tweets'])}
                        contentStyle={{ borderRadius: 12, border: '1px solid #e2e8f0', fontSize: 13 }}
                      />
                      <Area type="monotone" dataKey="positive_pct" stroke={data.color} strokeWidth={2.5} fill="url(#posGrad)" />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <Empty title={t('co.collecting')} hint={t('co.chartsAppear')} />
                )}
              </div>

              <div className="card">
                <h3>{t('co.split')}</h3>
                <p className="card-title">{t('co.posVsNeg')}</p>
                {data.tweets > 0 ? (
                  <>
                    <ResponsiveContainer width="100%" height={180}>
                      <PieChart>
                        <Pie data={pieData} dataKey="value" nameKey="name" innerRadius={55} outerRadius={80} paddingAngle={3} strokeWidth={0}>
                          {pieData.map((p, i) => (
                            <Cell key={i} fill={p.name === 'Positive' ? data.color : '#f87171'} />
                          ))}
                        </Pie>
                        <Tooltip formatter={(v, n) => [v, n]} contentStyle={{ borderRadius: 12, border: '1px solid #e2e8f0', fontSize: 13 }} />
                      </PieChart>
                    </ResponsiveContainer>
                    <div className="row" style={{ justifyContent: 'center', gap: 18, fontSize: 13 }}>
                      <span className="badge" style={{ background: `${data.color}20`, color: data.color }}>{t('overview.positive')} {data.positive}</span>
                      <span className="badge" style={{ background: '#fee2e2', color: 'var(--bad)' }}>{t('overview.negative')} {data.negative}</span>
                    </div>
                  </>
                ) : (
                  <Empty title={t('co.noTweets')} hint={t('co.startPipeline')} />
                )}
              </div>
            </div>

            <div className="grid overview" style={{ marginBottom: 16 }}>
              <div className="card">
                <h3>{t('co.sell')}</h3>
                <p className="card-title">{t('co.sellSub')}</p>
                {data.positiveTweets.length ? (
                  <ul style={{ margin: 0, paddingLeft: 18, color: 'var(--good)', lineHeight: 1.7 }}>
                    <li>
                      <b style={{ color: 'var(--ink)' }}>{data.positive}</b> {t('co.sellP1')}
                    </li>
                    <li>
                      {t('co.sellP2', {
                        d: (data.slope || 0) >= 0 ? t('co.rising') : t('co.softening'),
                        s: data.slope ?? 0,
                      })}
                    </li>
                  </ul>
                ) : (
                  <p className="text-muted">{t('co.enough')}</p>
                )}
              </div>

              <div className="card">
                <h3 className="row" style={{ gap: 8 }}>
                  <AlertTriangle size={17} color="var(--bad)" /> {t('co.hurts')}
                </h3>
                <p className="card-title">{t('co.hurtsSub')}</p>
                {data.impacts.length ? (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {data.impacts.map((imp) => (
                      <span key={imp.keyword} className="pill">
                        {imp.keyword} × {imp.count}
                      </span>
                    ))}
                  </div>
                ) : (
                  <p className="text-muted">{t('co.hurtsNone')}</p>
                )}
              </div>
            </div>

            <div className="card">
              <div className="row" style={{ marginBottom: 8 }}>
                <h3>{t('co.say', { name: data.name })}</h3>
                <div className="spacer" />
                <div className="tabs" style={{ margin: 0 }}>
                  <button className={`tab ${tab === 'positive' ? 'active' : ''}`} onClick={() => setTab('positive')}>
                    <ThumbsUp size={13} style={{ verticalAlign: '-2px' }} /> {t('co.favourable', { n: data.positive })}
                  </button>
                  <button className={`tab ${tab === 'negative' ? 'active' : ''}`} onClick={() => setTab('negative')}>
                    <AlertTriangle size={13} style={{ verticalAlign: '-2px' }} /> {t('co.negative', { n: data.negative })}
                  </button>
                </div>
              </div>

              {tweets.length === 0 ? (
                <Empty title={t('co.nothing')} hint={t('co.willShow')} />
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>{t('feed.time')}</th>
                        <th>{t('feed.tweet')}</th>
                        <th>{t('feed.prediction')}</th>
                        <th>{t('feed.source')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tweets.slice(0, 40).map((tweet, i) => (
                        <tr key={`${tweet.tweet_id}-${i}`}>
                          <td className="mono text-muted" style={{ whiteSpace: 'nowrap' }}>{formatTime(tweet.ingestion_ts_ms)}</td>
                          <td className="tweet-text">{tweet.text}</td>
                          <td style={{ whiteSpace: 'nowrap' }}>
                            <SentimentBadge sentiment={tweet.predicted_sentiment} />
                            {tab === 'negative' && <span className="pill" style={{ marginLeft: 6 }}>{t('co.impact')}</span>}
                          </td>
                          <td className="text-muted">{tweet.source === 'enrichment' ? t('co.brand') : t('co.dataset')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <p className="footer">
              {t('co.next')}: <b style={{ color: outlookColor }}>{data.forecast !== null ? t('co.nextVal', { v: data.forecast }) : t('co.pending')}</b> · live pipeline
            </p>
          </>
        )}
      </main>
    </div>
  );
}
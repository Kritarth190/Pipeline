import React, { useState, useEffect } from 'react';
import { Upload, LogIn, UserPlus, Database, FileText, BarChart3, Settings, LogOut, Download, Trash2, Play, AlertCircle, CheckCircle, Loader } from 'lucide-react';

// API Configuration
const API_BASE_URL = import.meta.env.VITE_API_URL;

const IndustrialPipelineApp = () => {
  const [currentUser, setCurrentUser] = useState(null);
  const [token, setToken] = useState(localStorage.getItem('token'));
  const [view, setView] = useState('login');
  const [files, setFiles] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [pipelineConfig, setPipelineConfig] = useState({
    validation_enabled: true,
    outlier_detection_enabled: true,
    normalization_enabled: true,
    feature_engineering_enabled: true,
    missing_value_threshold: 0.3,
    outlier_std_threshold: 3.0
  });
  
  // RAG preparation (tokenizer + embedder)
  const [ragConfig, setRagConfig] = useState({
    text_columns: [],
    chunk_tokens: 256,
    chunk_overlap: 32,
    embed: true,
    embedder: 'scratch',
    embedding_model: 'BAAI/bge-small-en-v1.5',
    max_rows: 2000
  });
  const [embedders, setEmbedders] = useState(null);
  const [ragFileId, setRagFileId] = useState('');
  const [columnInfo, setColumnInfo] = useState(null);
  const [importForm, setImportForm] = useState({ source: 'huggingface', dataset: '', max_rows: 2000, file: '' });

  const [loginForm, setLoginForm] = useState({ username: '', password: '' });
  const [registerForm, setRegisterForm] = useState({ username: '', email: '', password: '', confirmPassword: '' });
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);

  // Check authentication on mount
  useEffect(() => {
    if (token) {
      verifyToken();
    }
  }, []);

  // Load data when user logs in
  useEffect(() => {
    if (currentUser && token) {
      loadFiles();
      loadJobs();
      loadDefaultConfig();
    }
  }, [currentUser]);

  // While a RAG job is running, refresh the job list every 3 seconds
  const hasActiveJob = jobs.some(j => j.status === 'pending' || j.status === 'running');
  useEffect(() => {
    if (!hasActiveJob || !token) return;
    const timer = setInterval(() => { loadJobs(); loadFiles(); }, 3000);
    return () => clearInterval(timer);
  }, [hasActiveJob, token]);

  const authHeaders = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };

  // Find out which embedders the server can run, and pick a sensible default
  useEffect(() => {
    if (view !== 'rag' || !token || embedders) return;
    (async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/embedders`, { headers: authHeaders });
        if (!response.ok) return;
        const data = await response.json();
        setEmbedders(data);
        setRagConfig(cfg => data.scratch.available
          ? { ...cfg, embedder: 'scratch' }
          : data.hf_api.available
            ? { ...cfg, embedder: 'hf_api' }
            : { ...cfg, embed: false });
      } catch (err) {
        console.error('Failed to load embedders:', err);
      }
    })();
  }, [view]);

  const handleRagFileSelect = async (fileId) => {
    setRagFileId(fileId);
    setColumnInfo(null);
    if (!fileId) return;
    try {
      const response = await fetch(`${API_BASE_URL}/files/${fileId}/columns`, { headers: authHeaders });
      const data = await response.json();
      if (response.ok) {
        setColumnInfo(data);
        setRagConfig(cfg => ({ ...cfg, text_columns: data.suggested_text_columns }));
      } else {
        setError(data.message || 'Could not read the file columns');
      }
    } catch (err) {
      setError('Network error while reading columns');
    }
  };

  const handleImport = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      const response = await fetch(`${API_BASE_URL}/datasets/import`, {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({
          source: importForm.source,
          dataset: importForm.dataset,
          max_rows: parseInt(importForm.max_rows) || 2000,
          file: importForm.file || undefined
        })
      });
      const data = await response.json();
      if (response.ok) {
        setSuccess(`Imported ${data.import.rows} rows from ${data.import.dataset}`);
        await loadFiles();
        handleRagFileSelect(String(data.file.id));
        setTimeout(() => setSuccess(''), 4000);
      } else {
        setError(data.message || 'Import failed');
      }
    } catch (err) {
      setError('Network error during import');
    } finally {
      setLoading(false);
    }
  };

  const handleRagProcess = async () => {
    if (!ragFileId) return;
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      const response = await fetch(`${API_BASE_URL}/rag/process`, {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ file_id: parseInt(ragFileId), config: ragConfig })
      });
      const data = await response.json();
      if (response.ok) {
        setSuccess('RAG job started. Follow its progress under Results.');
        await loadJobs();
        loadFiles();
        setView('results');
        setTimeout(() => setSuccess(''), 4000);
      } else {
        setError(data.message || 'Could not start the RAG job');
      }
    } catch (err) {
      setError('Network error while starting the RAG job');
    } finally {
      setLoading(false);
    }
  };

  const toggleTextColumn = (name) => {
    setRagConfig(cfg => ({
      ...cfg,
      text_columns: cfg.text_columns.includes(name)
        ? cfg.text_columns.filter(c => c !== name)
        : [...cfg.text_columns, name]
    }));
  };

  const verifyToken = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/files`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (response.ok) {
        setView('dashboard');
      } else {
        localStorage.removeItem('token');
        setToken(null);
      }
    } catch (err) {
      console.error('Token verification failed:', err);
    }
  };

  const loadFiles = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/files`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (response.ok) {
        const data = await response.json();
        setFiles(data.files);
      }
    } catch (err) {
      console.error('Failed to load files:', err);
    }
  };

  const loadJobs = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (response.ok) {
        const data = await response.json();
        setJobs(data.jobs);
      }
    } catch (err) {
      console.error('Failed to load jobs:', err);
    }
  };

  const loadDefaultConfig = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/config/defaults`);
      if (response.ok) {
        const config = await response.json();
        setPipelineConfig(config);
      }
    } catch (err) {
      console.error('Failed to load config:', err);
    }
  };

  const handleRegister = async (e) => {
    e.preventDefault();
    setError('');
    setSuccess('');

    if (registerForm.password !== registerForm.confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    if (registerForm.password.length < 6) {
      setError('Password must be at least 6 characters');
      return;
    }

    setLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: registerForm.username,
          email: registerForm.email,
          password: registerForm.password
        })
      });

      const data = await response.json();

      if (response.ok) {
        setSuccess('Registration successful! Please login.');
        setRegisterForm({ username: '', email: '', password: '', confirmPassword: '' });
        setTimeout(() => {
          setView('login');
          setSuccess('');
        }, 2000);
      } else {
        setError(data.message || 'Registration failed');
      }
    } catch (err) {
      setError('Network error. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: loginForm.username,
          password: loginForm.password
        })
      });

      const data = await response.json();

      if (response.ok) {
        setToken(data.token);
        setCurrentUser(data.user);
        localStorage.setItem('token', data.token);
        setView('dashboard');
        setLoginForm({ username: '', password: '' });
      } else {
        setError(data.message || 'Login failed');
      }
    } catch (err) {
      setError('Network error. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    setCurrentUser(null);
    setToken(null);
    localStorage.removeItem('token');
    setView('login');
    setFiles([]);
    setJobs([]);
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await fetch(`${API_BASE_URL}/files/upload`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body: formData
      });

      const data = await response.json();

      if (response.ok) {
        setSuccess('File uploaded successfully!');
        loadFiles();
        setTimeout(() => setSuccess(''), 3000);
      } else {
        setError(data.message || 'Upload failed');
      }
    } catch (err) {
      setError('Network error during upload');
    } finally {
      setLoading(false);
      e.target.value = '';
    }
  };

  const handleProcess = async (fileId) => {
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await fetch(`${API_BASE_URL}/process`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          file_id: fileId,
          config: pipelineConfig
        })
      });

      const data = await response.json();

      if (response.ok) {
        setSuccess('Processing completed successfully!');
        loadFiles();
        loadJobs();
        setTimeout(() => setSuccess(''), 3000);
      } else {
        setError(data.message || data.error || 'Processing failed');
      }
    } catch (err) {
      setError('Network error during processing');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (fileId) => {
    if (!confirm('Are you sure you want to delete this file?')) return;

    try {
      const response = await fetch(`${API_BASE_URL}/files/${fileId}`, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${token}` }
      });

      if (response.ok) {
        setSuccess('File deleted successfully');
        loadFiles();
        loadJobs();
        setTimeout(() => setSuccess(''), 3000);
      }
    } catch (err) {
      setError('Failed to delete file');
    }
  };

  const handleDownload = async (jobId) => {
    try {
      const response = await fetch(`${API_BASE_URL}/jobs/${jobId}/download`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });

      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        const isRag = jobs.find(j => j.id === jobId)?.config?.mode === 'rag';
        a.download = isRag ? `rag_chunks_${jobId}.jsonl` : `processed_${jobId}.csv`;
        a.click();
      }
    } catch (err) {
      setError('Failed to download file');
    }
  };

  // Login View
  if (view === 'login' && !currentUser) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="bg-white rounded-lg shadow-xl p-8 w-full max-w-md">
          <div className="flex items-center justify-center mb-6">
            <Database className="w-12 h-12 text-indigo-600" />
          </div>
          <h1 className="text-3xl font-bold text-center mb-2 text-gray-800">Industrial Data Pipeline</h1>
          <p className="text-center text-gray-600 mb-8">Sign in to access your data processing dashboard</p>
          
          {error && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded mb-4 flex items-center gap-2">
              <AlertCircle className="w-5 h-5" />
              {error}
            </div>
          )}

          {success && (
            <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded mb-4 flex items-center gap-2">
              <CheckCircle className="w-5 h-5" />
              {success}
            </div>
          )}

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
              <input
                type="text"
                value={loginForm.username}
                onChange={(e) => setLoginForm({ ...loginForm, username: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
              <input
                type="password"
                value={loginForm.password}
                onChange={(e) => setLoginForm({ ...loginForm, password: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-indigo-600 text-white py-2 rounded-lg hover:bg-indigo-700 transition flex items-center justify-center gap-2 disabled:opacity-50"
            >
              {loading ? <Loader className="w-5 h-5 animate-spin" /> : <LogIn className="w-5 h-5" />}
              {loading ? 'Signing In...' : 'Sign In'}
            </button>
          </form>

          <div className="mt-6 text-center">
            <button
              onClick={() => { setView('register'); setError(''); }}
              className="text-indigo-600 hover:text-indigo-800 font-medium"
            >
              Don't have an account? Register here
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Register View
  if (view === 'register' && !currentUser) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="bg-white rounded-lg shadow-xl p-8 w-full max-w-md">
          <div className="flex items-center justify-center mb-6">
            <UserPlus className="w-12 h-12 text-indigo-600" />
          </div>
          <h1 className="text-3xl font-bold text-center mb-2 text-gray-800">Create Account</h1>
          <p className="text-center text-gray-600 mb-8">Register to start processing your data</p>
          
          {error && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded mb-4 flex items-center gap-2">
              <AlertCircle className="w-5 h-5" />
              {error}
            </div>
          )}

          {success && (
            <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded mb-4 flex items-center gap-2">
              <CheckCircle className="w-5 h-5" />
              {success}
            </div>
          )}

          <form onSubmit={handleRegister} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
              <input
                type="text"
                value={registerForm.username}
                onChange={(e) => setRegisterForm({ ...registerForm, username: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
              <input
                type="email"
                value={registerForm.email}
                onChange={(e) => setRegisterForm({ ...registerForm, email: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
              <input
                type="password"
                value={registerForm.password}
                onChange={(e) => setRegisterForm({ ...registerForm, password: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Confirm Password</label>
              <input
                type="password"
                value={registerForm.confirmPassword}
                onChange={(e) => setRegisterForm({ ...registerForm, confirmPassword: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                required
                disabled={loading}
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-indigo-600 text-white py-2 rounded-lg hover:bg-indigo-700 transition flex items-center justify-center gap-2 disabled:opacity-50"
            >
              {loading ? <Loader className="w-5 h-5 animate-spin" /> : <UserPlus className="w-5 h-5" />}
              {loading ? 'Creating Account...' : 'Create Account'}
            </button>
          </form>

          <div className="mt-6 text-center">
            <button
              onClick={() => { setView('login'); setError(''); setSuccess(''); }}
              className="text-indigo-600 hover:text-indigo-800 font-medium"
            >
              Already have an account? Sign in
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Dashboard View
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Database className="w-8 h-8 text-indigo-600" />
            <h1 className="text-2xl font-bold text-gray-800">Industrial Data Pipeline</h1>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-gray-600">Welcome, {currentUser?.username}</span>
            <button
              onClick={handleLogout}
              className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition"
            >
              <LogOut className="w-4 h-4" />
              Logout
            </button>
          </div>
        </div>
      </div>

      {/* Notifications */}
      {error && (
        <div className="max-w-7xl mx-auto px-4 pt-4">
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded flex items-center gap-2">
            <AlertCircle className="w-5 h-5" />
            {error}
          </div>
        </div>
      )}

      {success && (
        <div className="max-w-7xl mx-auto px-4 pt-4">
          <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded flex items-center gap-2">
            <CheckCircle className="w-5 h-5" />
            {success}
          </div>
        </div>
      )}

      {/* Navigation */}
      <div className="max-w-7xl mx-auto px-4 py-4">
        <div className="flex gap-2 mb-6">
          <button
            onClick={() => setView('dashboard')}
            className={`px-4 py-2 rounded-lg transition flex items-center gap-2 ${
              view === 'dashboard' ? 'bg-indigo-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50'
            }`}
          >
            <Upload className="w-4 h-4" />
            Upload & Process
          </button>
          <button
            onClick={() => setView('rag')}
            className={`px-4 py-2 rounded-lg transition flex items-center gap-2 ${
              view === 'rag' ? 'bg-indigo-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50'
            }`}
          >
            <Database className="w-4 h-4" />
            RAG Prep
          </button>
          <button
            onClick={() => setView('results')}
            className={`px-4 py-2 rounded-lg transition flex items-center gap-2 ${
              view === 'results' ? 'bg-indigo-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50'
            }`}
          >
            <BarChart3 className="w-4 h-4" />
            Results ({jobs.length})
          </button>
          <button
            onClick={() => setView('settings')}
            className={`px-4 py-2 rounded-lg transition flex items-center gap-2 ${
              view === 'settings' ? 'bg-indigo-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50'
            }`}
          >
            <Settings className="w-4 h-4" />
            Pipeline Settings
          </button>
        </div>

        {/* Upload & Process View */}
        {view === 'dashboard' && (
          <div className="space-y-6">
            {/* Upload Section */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
                <Upload className="w-5 h-5 text-indigo-600" />
                Upload Data File
              </h2>
              
              <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center hover:border-indigo-400 transition">
                <input
                  type="file"
                  accept=".csv,.txt"
                  onChange={handleFileUpload}
                  className="hidden"
                  id="file-upload"
                  disabled={loading}
                />
                <label htmlFor="file-upload" className="cursor-pointer">
                  <FileText className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                  <p className="text-gray-600 mb-2">Click to upload or drag and drop</p>
                  <p className="text-sm text-gray-500">CSV or TXT files (max 50MB)</p>
                </label>
              </div>
            </div>

            {/* Uploaded Files */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Uploaded Files</h2>
              
              {files.length === 0 ? (
                <p className="text-gray-500">No files uploaded yet</p>
              ) : (
                <div className="space-y-3">
                  {files.map(file => (
                    <div key={file.id} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <FileText className="w-5 h-5 text-indigo-600" />
                        <div>
                          <p className="font-medium">{file.filename}</p>
                          <p className="text-sm text-gray-500">
                            {(file.file_size / 1024).toFixed(2)} KB • {new Date(file.upload_date).toLocaleString()}
                          </p>
                        </div>
                      </div>
                      <div className="flex gap-2">
                        {file.status === 'uploaded' && (
                          <button
                            onClick={() => handleProcess(file.id)}
                            disabled={loading}
                            className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700 transition text-sm flex items-center gap-1 disabled:opacity-50"
                          >
                            {loading ? <Loader className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                            Process
                          </button>
                        )}
                        {file.status === 'processing' && (
                          <span className="px-4 py-2 bg-yellow-100 text-yellow-700 rounded text-sm flex items-center gap-1">
                            <Loader className="w-4 h-4 animate-spin" />
                            Processing...
                          </span>
                        )}
                        {file.status === 'processed' && (
                          <span className="px-4 py-2 bg-green-100 text-green-700 rounded text-sm flex items-center gap-1">
                            <CheckCircle className="w-4 h-4" />
                            Processed
                          </span>
                        )}
                        {file.status === 'error' && (
                          <span className="px-4 py-2 bg-red-100 text-red-700 rounded text-sm flex items-center gap-1">
                            <AlertCircle className="w-4 h-4" />
                            Error
                          </span>
                        )}
                        <button
                          onClick={() => handleDelete(file.id)}
                          className="p-2 text-red-600 hover:bg-red-50 rounded transition"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}

        {/* RAG Prep View */}
        {view === 'rag' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-1 flex items-center gap-2">
                <Database className="w-5 h-5 text-indigo-600" />
                1. Get a dataset
              </h2>
              <p className="text-sm text-gray-500 mb-4">
                Import from Hugging Face or Kaggle, or use a file you already uploaded.
              </p>
              <form onSubmit={handleImport} className="grid grid-cols-1 md:grid-cols-4 gap-3">
                <select
                  value={importForm.source}
                  onChange={(e) => setImportForm({ ...importForm, source: e.target.value })}
                  className="px-3 py-2 border border-gray-300 rounded"
                >
                  <option value="huggingface">Hugging Face</option>
                  <option value="kaggle">Kaggle</option>
                </select>
                <input
                  type="text"
                  required
                  value={importForm.dataset}
                  onChange={(e) => setImportForm({ ...importForm, dataset: e.target.value })}
                  placeholder={importForm.source === 'huggingface' ? 'e.g. imdb or user/dataset' : 'e.g. owner/dataset-name'}
                  className="px-3 py-2 border border-gray-300 rounded md:col-span-2"
                />
                <input
                  type="number"
                  min="1"
                  value={importForm.max_rows}
                  onChange={(e) => setImportForm({ ...importForm, max_rows: e.target.value })}
                  title="Max rows to import"
                  className="px-3 py-2 border border-gray-300 rounded"
                />
                {importForm.source === 'kaggle' && (
                  <input
                    type="text"
                    value={importForm.file}
                    onChange={(e) => setImportForm({ ...importForm, file: e.target.value })}
                    placeholder="CSV file name (optional)"
                    className="px-3 py-2 border border-gray-300 rounded md:col-span-3"
                  />
                )}
                <button
                  type="submit"
                  disabled={loading}
                  className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700 transition disabled:opacity-50 flex items-center justify-center gap-2 md:col-start-4"
                >
                  {loading ? <Loader className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                  Import
                </button>
              </form>
            </div>

            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">2. Choose the text to embed</h2>
              <select
                value={ragFileId}
                onChange={(e) => handleRagFileSelect(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded mb-4"
              >
                <option value="">Select a file...</option>
                {files.map(f => (
                  <option key={f.id} value={f.id}>{f.filename}</option>
                ))}
              </select>

              {columnInfo && (
                <div>
                  <p className="font-medium mb-2">Text columns</p>
                  <div className="flex flex-wrap gap-2 mb-2">
                    {columnInfo.columns.map(col => (
                      <label
                        key={col.name}
                        className={`px-3 py-1 rounded border text-sm cursor-pointer ${
                          ragConfig.text_columns.includes(col.name)
                            ? 'bg-indigo-600 text-white border-indigo-600'
                            : 'bg-white text-gray-700 border-gray-300'
                        }`}
                      >
                        <input
                          type="checkbox"
                          className="hidden"
                          checked={ragConfig.text_columns.includes(col.name)}
                          onChange={() => toggleTextColumn(col.name)}
                        />
                        {col.name}
                        {col.text_candidate && <span className="opacity-70"> · ~{Math.round(col.avg_chars)} chars</span>}
                      </label>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500">
                    Suggested columns are pre-selected. Other short columns (labels, dates, numbers) are kept as metadata.
                  </p>
                </div>
              )}
            </div>

            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">3. Tokenizer and embedder</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-4 bg-gray-50 rounded">
                  <label className="block font-medium mb-2">Chunk size: {ragConfig.chunk_tokens} tokens</label>
                  <input
                    type="range" min="64" max="512" step="32"
                    value={ragConfig.chunk_tokens}
                    onChange={(e) => setRagConfig({ ...ragConfig, chunk_tokens: parseInt(e.target.value) })}
                    className="w-full"
                  />
                </div>
                <div className="p-4 bg-gray-50 rounded">
                  <label className="block font-medium mb-2">Overlap: {ragConfig.chunk_overlap} tokens</label>
                  <input
                    type="range" min="0" max={Math.floor(ragConfig.chunk_tokens / 2)} step="8"
                    value={Math.min(ragConfig.chunk_overlap, Math.floor(ragConfig.chunk_tokens / 2))}
                    onChange={(e) => setRagConfig({ ...ragConfig, chunk_overlap: parseInt(e.target.value) })}
                    className="w-full"
                  />
                </div>
                <div className="p-4 bg-gray-50 rounded">
                  <label className="flex items-center gap-2 font-medium mb-2">
                    <input
                      type="checkbox"
                      checked={ragConfig.embed}
                      onChange={(e) => setRagConfig({ ...ragConfig, embed: e.target.checked })}
                    />
                    Create embeddings
                  </label>
                  <select
                    disabled={!ragConfig.embed}
                    value={ragConfig.embedder}
                    onChange={(e) => setRagConfig({ ...ragConfig, embedder: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded disabled:opacity-50 mb-2"
                  >
                    <option value="scratch">
                      Trained from scratch (free){embedders && !embedders.scratch.available ? ' - not installed' : ''}
                    </option>
                    <option value="hf_api">
                      Hugging Face API (paid credits){embedders && !embedders.hf_api.available ? ' - no token' : ''}
                    </option>
                  </select>
                  {ragConfig.embed && ragConfig.embedder === 'scratch' && embedders?.scratch.available && (
                    <p className="text-xs text-gray-500">
                      Model: {embedders.scratch.name} · {embedders.scratch.dim} dimensions
                    </p>
                  )}
                  {ragConfig.embed && ragConfig.embedder === 'scratch' && embedders && !embedders.scratch.available && (
                    <p className="text-xs text-amber-700">
                      No trained model on the server yet. Train one from your chunks (see the README) or untick embeddings.
                    </p>
                  )}
                  {ragConfig.embed && ragConfig.embedder === 'hf_api' && (
                    <input
                      type="text"
                      value={ragConfig.embedding_model}
                      onChange={(e) => setRagConfig({ ...ragConfig, embedding_model: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded"
                    />
                  )}
                </div>
                <div className="p-4 bg-gray-50 rounded">
                  <label className="block font-medium mb-2">Max rows to process</label>
                  <input
                    type="number" min="1"
                    value={ragConfig.max_rows}
                    onChange={(e) => setRagConfig({ ...ragConfig, max_rows: parseInt(e.target.value) || 1 })}
                    className="w-full px-3 py-2 border border-gray-300 rounded"
                  />
                </div>
              </div>

              <button
                onClick={handleRagProcess}
                disabled={loading || !ragFileId || ragConfig.text_columns.length === 0}
                className="mt-6 px-6 py-3 bg-indigo-600 text-white rounded hover:bg-indigo-700 transition disabled:opacity-50 flex items-center gap-2"
              >
                {loading ? <Loader className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                Build RAG dataset
              </button>
            </div>
          </div>
        )}

        {/* Results View */}
        {view === 'results' && (
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Processing Results</h2>
            
            {jobs.length === 0 ? (
              <p className="text-gray-500">No processed data yet</p>
            ) : (
              <div className="space-y-4">
                {jobs.map(job => (
                  <div key={job.id} className="border border-gray-200 rounded-lg p-4">
                    <div className="flex items-start justify-between mb-3">
                      <div>
                        <h3 className="font-semibold text-lg">{job.filename || `Job #${job.id}`}</h3>
                        <p className="text-sm text-gray-500">
                          {new Date(job.processed_date).toLocaleString()}
                        </p>
                        <span className={`inline-block px-2 py-1 rounded text-xs mt-1 ${
                          job.status === 'completed' ? 'bg-green-100 text-green-700' :
                          job.status === 'failed' ? 'bg-red-100 text-red-700' :
                          'bg-yellow-100 text-yellow-700'
                        }`}>
                          {job.status}
                        </span>
                      </div>
                      {job.status === 'completed' && (
                        <button
                          onClick={() => handleDownload(job.id)}
                          className="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700 transition text-sm"
                        >
                          <Download className="w-4 h-4" />
                          Download
                        </button>
                      )}
                    </div>

                    {job.config?.mode === 'rag' && (job.status === 'running' || job.status === 'pending') && (
                      <div className="mt-2">
                        <div className="w-full bg-gray-200 rounded h-2">
                          <div
                            className="bg-indigo-600 h-2 rounded transition-all"
                            style={{ width: `${Math.round((job.metrics?.progress || 0) * 100)}%` }}
                          />
                        </div>
                        <p className="text-sm text-gray-500 mt-1">
                          {Math.round((job.metrics?.progress || 0) * 100)}% · {job.metrics?.chunks || 0} chunks so far
                        </p>
                      </div>
                    )}

                    {job.config?.mode === 'rag' && job.metrics && job.status === 'completed' && (
                      <>
                        {job.metrics.warning && (
                          <div className="bg-yellow-50 border border-yellow-200 text-yellow-800 text-sm p-3 rounded mb-3">
                            {job.metrics.warning}
                          </div>
                        )}
                        <div className="grid grid-cols-3 gap-4 mb-3">
                          <div className="bg-blue-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Documents</p>
                            <p className="text-2xl font-bold text-blue-600">{job.metrics.documents}</p>
                          </div>
                          <div className="bg-green-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Chunks</p>
                            <p className="text-2xl font-bold text-green-600">{job.metrics.chunks}</p>
                          </div>
                          <div className="bg-indigo-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Embedding dim</p>
                            <p className="text-2xl font-bold text-indigo-600">{job.metrics.embedding_dim ?? '—'}</p>
                          </div>
                        </div>
                        <div className="bg-gray-50 p-3 rounded text-sm text-gray-600 space-y-1">
                          <p>Text columns: {job.metrics.text_columns?.join(', ')}</p>
                          <p>Tokenizer: {job.metrics.tokenizer} · avg {job.metrics.avg_chunk_tokens} tokens/chunk</p>
                          <p>Embedding model: {job.metrics.embedding_model || 'none (chunks only)'}</p>
                          <p>
                            Skipped: {job.metrics.documents_skipped_empty} empty, {job.metrics.documents_skipped_duplicate} duplicate
                            · {job.metrics.processing_time}s
                          </p>
                        </div>
                      </>
                    )}

                    {job.config?.mode !== 'rag' && job.metrics && job.status === 'completed' && (
                      <>
                        <div className="grid grid-cols-3 gap-4 mb-3">
                          <div className="bg-blue-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Total Records</p>
                            <p className="text-2xl font-bold text-blue-600">{job.metrics.total_records}</p>
                          </div>
                          <div className="bg-green-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Valid Records</p>
                            <p className="text-2xl font-bold text-green-600">{job.metrics.valid_records}</p>
                          </div>
                          <div className="bg-red-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Invalid Records</p>
                            <p className="text-2xl font-bold text-red-600">{job.metrics.invalid_records}</p>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4 mb-3">
                          <div className="bg-gray-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Processing Time</p>
                            <p className="text-lg font-semibold">{job.metrics.processing_time?.toFixed(2)}s</p>
                          </div>
                          <div className="bg-gray-50 p-3 rounded">
                            <p className="text-sm text-gray-600">Data Quality</p>
                            <p className="text-lg font-semibold">{(job.metrics.data_quality_score * 100).toFixed(1)}%</p>
                          </div>
                        </div>

                        {job.metrics.stage_times && (
                          <div className="bg-gray-50 p-3 rounded">
                            <p className="text-sm font-medium mb-2">Stage Execution Times:</p>
                            <div className="space-y-1">
                              {Object.entries(job.metrics.stage_times).map(([stage, time]) => (
                                <p key={stage} className="text-sm text-gray-600">
                                  {stage}: {time.toFixed(2)}s
                                </p>
                              ))}
                            </div>
                          </div>
                        )}
                      </>
                    )}

                    {job.error_message && job.status === 'failed' && (
                      <div className="bg-red-50 p-3 rounded mt-2">
                        <p className="text-sm font-medium text-red-700">Error:</p>
                        <p className="text-sm text-red-600">{job.error_message}</p>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Settings View */}
        {view === 'settings' && (
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Pipeline Configuration</h2>
            
            <div className="space-y-4">
              <div className="flex items-center justify-between p-4 bg-gray-50 rounded">
                <div>
                  <p className="font-medium">Validation</p>
                  <p className="text-sm text-gray-600">Validate data types and required fields</p>
                </div>
                <label className="relative inline-block w-12 h-6">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.validation_enabled}
                    onChange={(e) => setPipelineConfig({ ...pipelineConfig, validation_enabled: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute inset-0 bg-gray-300 rounded-full peer-checked:bg-indigo-600 transition cursor-pointer"></span>
                  <span className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition peer-checked:translate-x-6"></span>
                </label>
              </div>

              <div className="flex items-center justify-between p-4 bg-gray-50 rounded">
                <div>
                  <p className="font-medium">Outlier Detection</p>
                  <p className="text-sm text-gray-600">Remove statistical outliers from data</p>
                </div>
                <label className="relative inline-block w-12 h-6">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.outlier_detection_enabled}
                    onChange={(e) => setPipelineConfig({ ...pipelineConfig, outlier_detection_enabled: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute inset-0 bg-gray-300 rounded-full peer-checked:bg-indigo-600 transition cursor-pointer"></span>
                  <span className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition peer-checked:translate-x-6"></span>
                </label>
              </div>

              <div className="flex items-center justify-between p-4 bg-gray-50 rounded">
                <div>
                  <p className="font-medium">Normalization</p>
                  <p className="text-sm text-gray-600">Scale numeric features to 0-1 range</p>
                </div>
                <label className="relative inline-block w-12 h-6">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.normalization_enabled}
                    onChange={(e) => setPipelineConfig({ ...pipelineConfig, normalization_enabled: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute inset-0 bg-gray-300 rounded-full peer-checked:bg-indigo-600 transition cursor-pointer"></span>
                  <span className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition peer-checked:translate-x-6"></span>
                </label>
              </div>

              <div className="flex items-center justify-between p-4 bg-gray-50 rounded">
                <div>
                  <p className="font-medium">Feature Engineering</p>
                  <p className="text-sm text-gray-600">Create derived features from existing data</p>
                </div>
                <label className="relative inline-block w-12 h-6">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.feature_engineering_enabled}
                    onChange={(e) => setPipelineConfig({ ...pipelineConfig, feature_engineering_enabled: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute inset-0 bg-gray-300 rounded-full peer-checked:bg-indigo-600 transition cursor-pointer"></span>
                  <span className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition peer-checked:translate-x-6"></span>
                </label>
              </div>

              <div className="p-4 bg-gray-50 rounded">
                <label className="block font-medium mb-2">
                  Missing Value Threshold: {pipelineConfig.missing_value_threshold}
                </label>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.1"
                  value={pipelineConfig.missing_value_threshold}
                  onChange={(e) => setPipelineConfig({ ...pipelineConfig, missing_value_threshold: parseFloat(e.target.value) })}
                  className="w-full"
                />
                <p className="text-sm text-gray-600 mt-1">
                  Columns with more than {(pipelineConfig.missing_value_threshold * 100).toFixed(0)}% missing values will be dropped
                </p>
              </div>

              <div className="p-4 bg-gray-50 rounded">
                <label className="block font-medium mb-2">
                  Outlier Std Threshold: {pipelineConfig.outlier_std_threshold}
                </label>
                <input
                  type="range"
                  min="1"
                  max="5"
                  step="0.5"
                  value={pipelineConfig.outlier_std_threshold}
                  onChange={(e) => setPipelineConfig({ ...pipelineConfig, outlier_std_threshold: parseFloat(e.target.value) })}
                  className="w-full"
                />
                <p className="text-sm text-gray-600 mt-1">
                  Values beyond {pipelineConfig.outlier_std_threshold} standard deviations will be considered outliers
                </p>
              </div>

              <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded">
                <p className="text-sm text-blue-700">
                  <strong>Note:</strong> Configuration changes will apply to the next processing job.
                </p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default IndustrialPipelineApp;
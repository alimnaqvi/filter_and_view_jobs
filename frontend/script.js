// frontend/script.js
document.addEventListener('DOMContentLoaded', () => {
    const jobList = document.getElementById('job-list');
    const tableHeaderRow = document.getElementById('table-header-row');
    const statusFilter = document.getElementById('status-filter');
    const daysSinceSavedFilter = document.getElementById('time-since-saved-filter');
    const germanFilter = document.getElementById('german-filter');
    const seniorityFilter = document.getElementById('seniority-filter');
    const searchBox = document.getElementById('search-box');
    const refCacheBtn = document.getElementById('refcache-button');
    const jobCount = document.getElementById('job-count');
    // TODO: After adding URL to Gemini DB, show a button to go the original link
    // Todo: Show dates in different colors?

    // Define table columns configuration
    const columns = [
        { header: 'Status', type: 'status' },
        { header: 'Actions', type: 'actions' },
        { header: 'Job Title', key: 'Job title' },
        { header: 'German required', key: 'German language fluency required' },
        { header: 'Job description language', key: 'Job description language', className: 'col-narrow' },
        { header: 'English proficiency mentioned', key: 'English proficiency mentioned', className: 'col-narrow' },
        { header: 'Is tech job', key: 'Is tech job' },
        { header: 'Role seniority', key: 'Role seniority' },
        { header: 'Company', key: 'Company name' },
        { header: 'Location', key: 'Location' },
        { header: 'Job added time', key: 'last_mod_time', type: 'date' },
        { header: 'Required technical skills', key: 'Required technical skills', className: 'col-wide' },
        { header: 'Preferred technical skills', key: 'Preferred technical skills', className: 'col-wide' },
        { header: 'Other skills mentioned', key: 'Other skills mentioned', className: 'col-wide' },
        { header: 'Immatrikulation required', key: 'Immatrikulation required', className: 'col-narrow' },
        { header: 'Experience mentioned', key: 'Experience mentioned' },
        { header: 'Other requirements', key: 'Other requirements' },
        { header: 'Tech stack', key: 'Tech stack', className: 'col-wide' },
        { header: 'Job category', key: 'Job category' }
    ];

    let searchTimeout;
    let refCache = false;
    // ? (If slow) Implement local database cache and add a button to refresh the cache

    const getSelectedValues = (select) => {
        return Array.from(select.selectedOptions).map(option => option.value);
    };

    const fetchAndRenderJobs = async () => {
        const status = statusFilter.value;
        const daysSinceSaved = daysSinceSavedFilter.value;
        const germanValues = getSelectedValues(germanFilter);
        const seniorityValues = getSelectedValues(seniorityFilter);
        const query = searchBox.value;

        // Build the API URL with query parameters
        const url = new URL('/api/jobs', window.location.origin);
        if (status && status !== 'all') {
            url.searchParams.append('status', status);
        }
        if (daysSinceSaved) {
            url.searchParams.append('days', daysSinceSaved);
        }
        if (germanValues.length && !germanValues.includes('all')) {
            germanValues.forEach(val => url.searchParams.append('german', val));
        }
        if (seniorityValues.length && !seniorityValues.includes('all')) {
            seniorityValues.forEach(val => url.searchParams.append('seniority', val));
        }
        if (query) {
            url.searchParams.append('q', query);
        }
        if (refCache) {
            url.searchParams.append('refcache', "true");
            refCache = false;
        }

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const jobs = await response.json();
            renderJobs(jobs);
        } catch (error) {
            console.error("Failed to fetch jobs:", error);
            jobList.innerHTML = `<tr><td colspan="6" style="color: red; text-align: center;">Error loading jobs. Is the backend server running?</td></tr>`;
            jobCount.textContent = 'Could not load jobs.';
        }
    };

    const renderHeaders = () => {
        tableHeaderRow.innerHTML = '';
        columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col.header;
            if (col.className) {
                th.classList.add(col.className);
            }
            tableHeaderRow.appendChild(th);
        });
    };

    const createCell = (text) => {
        const cell = document.createElement('td');
        cell.textContent = text || 'N/A'; // Handle empty values
        return cell;
    };

    const renderActions = (actionsCell, job, row) => {
        // Link to the saved HTML file
        const viewHtmlBtn = document.createElement('a');
        viewHtmlBtn.href = `/jobs/${job.Filename}`; // Served by FastAPI's StaticFiles
        viewHtmlBtn.textContent = 'View HTML';
        viewHtmlBtn.target = '_blank'; // Open in new tab
        viewHtmlBtn.className = 'action-btn btn-view';
        actionsCell.appendChild(viewHtmlBtn);

        // Link to original URL
        if (job['Job URL'] && job['Job URL'] != "N/A") {
            const viewUrlBtn = document.createElement('a');
            viewUrlBtn.href = job['Job URL'];
            viewUrlBtn.textContent = 'Go to URL';
            viewUrlBtn.target = '_blank'; // Open in new tab
            viewUrlBtn.rel = 'noreferrer noopener' // tell the browser not to send the Referer header
            viewUrlBtn.className = 'action-btn btn-url';
            actionsCell.appendChild(viewUrlBtn);
        }

        // Button to quickly mark as viewed
        if (job.status === 'new') {
            const viewedBtn = document.createElement('a');
            viewedBtn.textContent = 'Viewed';
            viewedBtn.className = 'action-btn btn-viewed';
            viewedBtn.onclick = () => {
                if (!statusFilter.value.includes('viewed')) {
                    // Immediately make row invisible if the updated status isn't part of the currently selected filter
                    row.style.display = 'none';
                }
                updateStatus(job.Filename, 'viewed');
            }
            actionsCell.appendChild(viewedBtn);
        }

        // Add dropdown to change status
        const statusArr = ["", "New", "Viewed", "Shortlisted", "Longlisted", "Applied", "Archived"];
        const statusDropdown = document.createElement('select');
        statusDropdown.id = 'change-status-filter';
        statusDropdown.className = 'change-status-filter';
        statusArr.forEach((item) => {
            const opt = document.createElement('option');
            opt.value = item.toLowerCase();
            if (item === "") {
                opt.textContent = "Change status"
            } else {
                opt.textContent = item;
                opt.onclick = () => {
                    if (!statusFilter.value.includes(item.toLowerCase())) {
                        // Immediately make row invisible if the updated status isn't part of the currently selected filter
                        row.style.display = 'none';
                    }
                    updateStatus(job.Filename, item.toLowerCase());
                }
            }
            statusDropdown.appendChild(opt);
        });
        actionsCell.appendChild(statusDropdown);
    };

    const renderJobs = (jobs) => {
        jobList.innerHTML = ''; // Clear existing list

        if (jobs.length === 0) {
            jobList.innerHTML = `<tr><td colspan="${columns.length}" style="text-align: center;">No jobs found.</td></tr>`;
            jobCount.textContent = '0 jobs found.';
            return;
        }

        jobCount.textContent = `${jobs.length} job(s) found.`;

        jobs.forEach(job => {
            const row = document.createElement('tr');
            
            columns.forEach(col => {
                let cell;
                if (col.type === 'status') {
                    cell = document.createElement('td');
                    const statusClass = `status-${job.status || 'default'}`;
                    cell.innerHTML = `<span class="status-badge ${statusClass}">${job.status}</span>`;
                } else if (col.type === 'actions') {
                    cell = document.createElement('td');
                    renderActions(cell, job, row);
                } else if (col.type === 'date') {
                    const last_mod_date = new Date(job[col.key]);
                    const date_options = {
                        weekday: "short",
                        day: "numeric",
                        month: "short",
                        year: "numeric",
                        hour: "2-digit",
                        minute: "2-digit",
                        timeZoneName: "short",
                    };
                    const dateStr = last_mod_date.toLocaleDateString("en-DE", date_options);
                    cell = createCell(dateStr);
                } else {
                    cell = createCell(job[col.key]);
                }

                if (col.className) {
                    cell.classList.add(col.className);
                }
                row.appendChild(cell);
            });

            jobList.appendChild(row);
        });
    };

    const updateStatus = async (filename, newStatus) => {
        try {
            const response = await fetch(`/api/jobs/${filename}/status`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ status: newStatus }),
            });

            if (!response.ok) {
                throw new Error('Failed to update status');
            }
            // Refresh the list to show the change
            fetchAndRenderJobs();
        } catch (error) {
            console.error("Error updating status:", error);
            alert('Could not update job status.');
        }
    };

    // Event Listeners for filters
    statusFilter.addEventListener('change', fetchAndRenderJobs);
    daysSinceSavedFilter.addEventListener('change', fetchAndRenderJobs);
    germanFilter.addEventListener('change', fetchAndRenderJobs);
    seniorityFilter.addEventListener('change', fetchAndRenderJobs);
    refCacheBtn.addEventListener('click', () => {
        refCache = true;
        fetchAndRenderJobs();
    });
    searchBox.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        // Debounce search input to avoid excessive API calls
        searchTimeout = setTimeout(fetchAndRenderJobs, 300);
    });

    // Initial load
    renderHeaders();
    fetchAndRenderJobs();
});

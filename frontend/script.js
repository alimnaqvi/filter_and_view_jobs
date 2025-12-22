// frontend/script.js
document.addEventListener('DOMContentLoaded', () => {
    const jobList = document.getElementById('job-list');
    const statusFilter = document.getElementById('status-filter');
    const germanFilter = document.getElementById('german-filter');
    const seniorityFilter = document.getElementById('seniority-filter');
    const searchBox = document.getElementById('search-box');
    const refCacheBtn = document.getElementById('refcache-button');
    const jobCount = document.getElementById('job-count');
    // TODO: After adding URL to Gemini DB, show a button to go the original link
    // Todo: Show dates in different colors?

    let searchTimeout;
    let refCache = false;
    // ? (If slow) Implement local database cache and add a button to refresh the cache

    const getSelectedValues = (select) => {
        return Array.from(select.selectedOptions).map(option => option.value);
    };

    const fetchAndRenderJobs = async () => {
        const status = statusFilter.value;
        const germanValues = getSelectedValues(germanFilter);
        const seniorityValues = getSelectedValues(seniorityFilter);
        const query = searchBox.value;

        // Build the API URL with query parameters
        const url = new URL('/api/jobs', window.location.origin);
        if (status && status !== 'all') {
            url.searchParams.append('status', status);
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

    const renderJobs = (jobs) => {
        jobList.innerHTML = ''; // Clear existing list

        if (jobs.length === 0) {
            jobList.innerHTML = `<tr><td colspan="6" style="text-align: center;">No jobs found.</td></tr>`;
            jobCount.textContent = '0 jobs found.';
            return;
        }

        jobCount.textContent = `${jobs.length} job(s) found.`;

        jobs.forEach(job => {
            const row = document.createElement('tr');
            
            // Status Badge
            const statusCell = document.createElement('td');
            const statusClass = `status-${job.status || 'default'}`;
            statusCell.innerHTML = `<span class="status-badge ${statusClass}">${job.status}</span>`;

            // Simple cell for other data
            const createCell = (text) => {
                const cell = document.createElement('td');
                cell.textContent = text || 'N/A'; // Handle empty values
                return cell;
            };

            // Actions Cell
            const actionsCell = document.createElement('td');

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
            // // Label for the dropdown
            // const statusLabel = document.createElement('label');
            // statusLabel.for = 'change-status-filter';
            // statusLabel.textContent = 'Change status';
            // actionsCell.appendChild(statusLabel);
            // The dropdown
            statusArr = ["", "Viewed", "New", "Shortlisted", "Longlisted", "Applied", "Archived"];
            const statusDropdown = document.createElement('select');
            statusDropdown.id = 'change-status-filter';
            statusDropdown.className = 'change-status-filter';
            statusArr.forEach((item) => {
                opt = document.createElement('option');
                opt.value = item.toLowerCase();
                if (item === "") {
                    opt.textContent = "Change status"
                } else {
                    opt.textContent = item;
                    opt.onclick = () => {
                        if (!statusFilter.value.includes(opt.value)) {
                            // Immediately make row invisible if the updated status isn't part of the currently selected filter
                            row.style.display = 'none';
                        }
                        updateStatus(job.Filename, opt.value);
                    }
                }
                statusDropdown.appendChild(opt);
            });
            actionsCell.appendChild(statusDropdown);
            
            // // Add buttons to change view status
            // const viewStatus = (job.status === 'new') ? 'viewed' : 'new';
            // const updateViewedButton = document.createElement('button');
            // updateViewedButton.textContent = `Mark as ${viewStatus}`;
            // updateViewedButton.className = 'action-btn btn-update';
            // updateViewedButton.onclick = () => updateStatus(job.Filename, viewStatus);
            // actionsCell.appendChild(updateViewedButton);

            // // Add buttons to change shortlist status
            // const shortlistStatus = (job.status !== 'shortlisted') ? 'shortlisted' : 'new';
            // const updateShortlistedButton = document.createElement('button');
            // updateShortlistedButton.textContent = `Mark as ${shortlistStatus}`;
            // updateShortlistedButton.className = 'action-btn btn-update';
            // updateShortlistedButton.onclick = () => updateStatus(job.Filename, shortlistStatus);
            // actionsCell.appendChild(updateShortlistedButton);

            // Date added
            last_mod_date = new Date(job['last_mod_time'])
            const date_options = {
                weekday: "short",
                day: "numeric",
                month: "short",
                year: "numeric",
                hour: "2-digit",
                minute: "2-digit",
                timeZoneName: "short",
                // hourCycle: "h23",
            };
            job_added_date = last_mod_date.toLocaleDateString("en-DE", date_options)

            row.appendChild(statusCell);
            row.appendChild(actionsCell);
            row.appendChild(createCell(job['Job title']));
            row.appendChild(createCell(job['German language fluency required']));
            row.appendChild(createCell(job['Is tech job']));
            row.appendChild(createCell(job['Role seniority']));
            row.appendChild(createCell(job['Company name']));
            row.appendChild(createCell(job['Location']));
            row.appendChild(createCell(job_added_date));
            row.appendChild(createCell(job['Required technical skills']));
            row.appendChild(createCell(job['Preferred technical skills']));
            row.appendChild(createCell(job['Immatrikulation required']));
            row.appendChild(createCell(job['Other requirements']));
            row.appendChild(createCell(job['Job category']));

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
    fetchAndRenderJobs();
});

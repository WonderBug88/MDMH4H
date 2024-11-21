from flask import (
    Blueprint, render_template, request, flash, redirect, url_for, session
)
from werkzeug.security import generate_password_hash, check_password_hash
from app.config import Config
from db.curd import DataRetriever

user_bp = Blueprint("user", __name__)

# Ensure schema is specified
schema_name = 'h4h_import2'


@user_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')

        db = DataRetriever(schema=schema_name)

        # check if user exist in database first
        check_user_query = f"SELECT * FROM users WHERE email = %s;"
        is_user = db.check_if_exists(check_user_query, (email,))
        if not is_user:
            flash('Invalid credentials. Please try again.', 'danger')
            return redirect(url_for('user.login'))

        query = f"SELECT * FROM users WHERE email = %s;"
        user = db.get_one(query, (email,))

        # Validate credentials
        if user and check_password_hash(user['password_hash'], password):
            session['logged_in'] = True
            session['email'] = email
            session['name'] = f"{user['first_name']} {user['last_name']}"
            flash('Login successful!', 'success')
            return redirect(url_for('pam.pam_main'))
        else:
            flash('Invalid credentials. Please try again.', 'danger')
            return redirect(url_for('user.login'))

    if 'logged_in' in session:
        return redirect(url_for('pam.pam_main'))
    return render_template('login.html')


@user_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        name = request.form.get('name', '').split(' ')
        first_name = name[0] if len(name) > 0 else ''
        last_name = name[1] if len(name) > 1 else ''
        password = request.form.get('password')
        email = request.form.get('email')
        username = request.form.get('username')

        # Hash the password
        hashed_password = generate_password_hash(password)

        # Save user to database
        db = DataRetriever(schema=schema_name)
        query = f"""
        INSERT INTO users (first_name, last_name, username, email, password_hash, is_admin, is_active)
        VALUES (%s, %s, %s, %s, %s, false, true)
        RETURNING user_id;
        """
        try:
            db.execute_commit_query(query, (first_name, last_name, username, email, hashed_password))
            flash('User registered successfully!', 'success')
            return redirect(url_for('user.login'))
        except Exception as e:
            db.close()
            flash(f'Error registering user: {e}', 'danger')
            return redirect(url_for('user.register'))

    return render_template('register.html')


@user_bp.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('email', None)
    session.pop('name', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('user.login'))

"""Monthly summary calculation utilities for bank statement data."""

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from src.pdf_processor.extractor import TransactionData
from src.utils.logger import get_logger


class SummaryCalculationError(Exception):
    """Custom exception for summary calculation errors."""
    pass


class MonthlySummarizer:
    """Handles calculation of monthly summaries for bank statements."""
    
    def __init__(self) -> None:
        """Initialize monthly summarizer."""
        self.logger = get_logger(__name__)
    
    def group_transactions_by_month(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, List[TransactionData]]:
        """Group transactions by month.
        
        Args:
            transactions: List of TransactionData objects.
            
        Returns:
            Dictionary with month keys and transaction lists as values.
        """
        monthly_groups = defaultdict(list)
        
        for transaction in transactions:
            try:
                # Parse date and extract month
                date_obj = datetime.strptime(transaction.date, "%Y-%m-%d")
                month_key = date_obj.strftime("%Y-%m")
                monthly_groups[month_key].append(transaction)
                
            except ValueError as e:
                self.logger.warning(f"Failed to parse date {transaction.date}: {str(e)}")
                continue
        
        # Convert defaultdict to regular dict and sort by month
        result = dict(sorted(monthly_groups.items()))
        
        self.logger.info(f"Grouped transactions into {len(result)} months")
        return result
    
    def calculate_monthly_totals(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Any]:
        """Calculate total credits, debits, and net for a month.
        
        Args:
            transactions: List of transactions for a specific month.
            
        Returns:
            Dictionary with monthly totals.
        """
        total_credits = Decimal('0')
        total_debits = Decimal('0')
        transaction_count = len(transactions)
        
        for transaction in transactions:
            if transaction.credit:
                total_credits += transaction.credit
            if transaction.debit:
                total_debits += transaction.debit
        
        net_amount = total_credits - total_debits
        
        return {
            "transaction_count": transaction_count,
            "total_credits": float(total_credits),
            "total_debits": float(total_debits),
            "net_amount": float(net_amount),
            "average_credit": float(total_credits / max(transaction_count, 1)),
            "average_debit": float(total_debits / max(transaction_count, 1)),
        }
    
    def calculate_monthly_summary(
        self,
        month_key: str,
        transactions: List[TransactionData]
    ) -> Dict[str, Any]:
        """Calculate comprehensive summary for a specific month.
        
        Args:
            month_key: Month identifier (YYYY-MM).
            transactions: List of transactions for the month.
            
        Returns:
            Dictionary with comprehensive monthly summary.
        """
        if not transactions:
            return {
                "month": month_key,
                "transaction_count": 0,
                "total_credits": 0.0,
                "total_debits": 0.0,
                "net_amount": 0.0,
            }
        
        # Basic totals
        totals = self.calculate_monthly_totals(transactions)
        
        # Additional analysis
        daily_totals = self._calculate_daily_totals(transactions)
        top_transactions = self._get_top_transactions(transactions)
        category_analysis = self._analyze_transaction_categories(transactions)
        
        # Balance analysis
        balance_analysis = self._analyze_balance_changes(transactions)
        
        summary = {
            "month": month_key,
            **totals,
            "daily_average_transactions": len(daily_totals) / max(len(daily_totals), 1),
            "highest_single_credit": self._get_highest_amount(transactions, "credit"),
            "highest_single_debit": self._get_highest_amount(transactions, "debit"),
            "top_credits": top_transactions["credits"][:5],
            "top_debits": top_transactions["debits"][:5],
            "category_breakdown": category_analysis,
            "balance_analysis": balance_analysis,
        }
        
        return summary
    
    def _calculate_daily_totals(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Dict[str, Decimal]]:
        """Calculate daily totals for transactions.
        
        Args:
            transactions: List of transactions.
            
        Returns:
            Dictionary with date keys and daily totals.
        """
        daily_totals = defaultdict(lambda: {"credits": Decimal('0'), "debits": Decimal('0')})
        
        for transaction in transactions:
            if transaction.credit:
                daily_totals[transaction.date]["credits"] += transaction.credit
            if transaction.debit:
                daily_totals[transaction.date]["debits"] += transaction.debit
        
        return dict(daily_totals)
    
    def _get_top_transactions(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get top transactions by amount.
        
        Args:
            transactions: List of transactions.
            
        Returns:
            Dictionary with top credits and debits as dictionaries.
        """
        credits = [t for t in transactions if t.credit]
        debits = [t for t in transactions if t.debit]
        
        # Sort by amount (descending)
        credits.sort(key=lambda x: x.credit, reverse=True)
        debits.sort(key=lambda x: x.debit, reverse=True)
        
        # Convert to dictionaries for Excel compatibility
        credit_dicts = [
            {
                "date": t.date,
                "description": t.description,
                "amount": float(t.credit) if t.credit else 0.0
            }
            for t in credits
        ]
        
        debit_dicts = [
            {
                "date": t.date,
                "description": t.description,
                "amount": float(t.debit) if t.debit else 0.0
            }
            for t in debits
        ]
        
        return {
            "credits": credit_dicts,
            "debits": debit_dicts,
        }
    
    def _analyze_transaction_categories(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Dict[str, Any]]:
        """Analyze transactions by category based on description.
        
        Args:
            transactions: List of transactions.
            
        Returns:
            Dictionary with category analysis.
        """
        categories = defaultdict(lambda: {"count": 0, "total": Decimal('0'), "type": "unknown"})
        
        # Simple category keywords
        category_keywords = {
            "salary": ["salary", "payroll", "wage"],
            "utilities": ["electric", "water", "gas", "utility"],
            "food": ["restaurant", "grocery", "food", "cafe"],
            "transport": ["fuel", "gas", "taxi", "uber", "bus", "train"],
            "shopping": ["store", "shop", "retail", "amazon"],
            "banking": ["fee", "charge", "interest", "transfer"],
            "atm": ["atm", "cash"],
        }
        
        for transaction in transactions:
            description_lower = transaction.description.lower()
            amount = transaction.credit or transaction.debit
            transaction_type = "credit" if transaction.credit else "debit"
            
            categorized = False
            for category, keywords in category_keywords.items():
                if any(keyword in description_lower for keyword in keywords):
                    categories[category]["count"] += 1
                    categories[category]["total"] += amount
                    categories[category]["type"] = transaction_type
                    categorized = True
                    break
            
            if not categorized:
                categories["other"]["count"] += 1
                categories["other"]["total"] += amount
                categories["other"]["type"] = transaction_type
        
        # Convert to regular dict and calculate percentages
        total_transactions = len(transactions)
        result = {}
        
        for category, data in categories.items():
            result[category] = {
                "count": data["count"],
                "total": float(data["total"]),
                "percentage": (data["count"] / total_transactions) * 100 if total_transactions > 0 else 0,
                "type": data["type"],
            }
        
        return result
    
    def _analyze_balance_changes(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Any]:
        """Analyze balance changes over the month.
        
        Args:
            transactions: List of transactions.
            
        Returns:
            Dictionary with balance analysis.
        """
        balances = [t.balance for t in transactions if t.balance is not None]
        
        if not balances:
            return {
                "opening_balance": None,
                "closing_balance": None,
                "balance_change": None,
                "peak_balance": None,
                "lowest_balance": None,
            }
        
        opening_balance = balances[0]
        closing_balance = balances[-1]
        balance_change = closing_balance - opening_balance
        peak_balance = max(balances)
        lowest_balance = min(balances)
        
        return {
            "opening_balance": float(opening_balance),
            "closing_balance": float(closing_balance),
            "balance_change": float(balance_change),
            "peak_balance": float(peak_balance),
            "lowest_balance": float(lowest_balance),
        }
    
    def _get_highest_amount(
        self,
        transactions: List[TransactionData],
        transaction_type: str
    ) -> Optional[float]:
        """Get highest amount for specified transaction type.
        
        Args:
            transactions: List of transactions.
            transaction_type: Either "credit" or "debit".
            
        Returns:
            Highest amount or None if no transactions of that type.
        """
        amounts = []
        
        if transaction_type == "credit":
            amounts = [float(t.credit) for t in transactions if t.credit]
        elif transaction_type == "debit":
            amounts = [float(t.debit) for t in transactions if t.debit]
        
        return max(amounts) if amounts else None
    
    def generate_comprehensive_summary(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Any]:
        """Generate comprehensive summary for all transactions.
        
        Args:
            transactions: List of all transactions.
            
        Returns:
            Dictionary with comprehensive summary.
        """
        if not transactions:
            return {
                "total_transactions": 0,
                "monthly_summaries": {},
                "overall_totals": {
                    "total_credits": 0.0,
                    "total_debits": 0.0,
                    "net_amount": 0.0,
                },
                "analysis_period": None,
            }
        
        # Group by month
        monthly_groups = self.group_transactions_by_month(transactions)
        
        # Calculate monthly summaries
        monthly_summaries = {}
        overall_credits = Decimal('0')
        overall_debits = Decimal('0')
        
        for month_key, month_transactions in monthly_groups.items():
            summary = self.calculate_monthly_summary(month_key, month_transactions)
            monthly_summaries[month_key] = summary
            
            overall_credits += Decimal(str(summary["total_credits"]))
            overall_debits += Decimal(str(summary["total_debits"]))
        
        # Calculate overall totals
        overall_net = overall_credits - overall_debits
        
        # Determine analysis period
        dates = [t.date for t in transactions]
        analysis_period = {
            "start_date": min(dates),
            "end_date": max(dates),
            "total_days": (datetime.strptime(max(dates), "%Y-%m-%d") - 
                          datetime.strptime(min(dates), "%Y-%m-%d")).days + 1
        }
        
        comprehensive_summary = {
            "total_transactions": len(transactions),
            "monthly_summaries": monthly_summaries,
            "overall_totals": {
                "total_credits": float(overall_credits),
                "total_debits": float(overall_debits),
                "net_amount": float(overall_net),
            },
            "analysis_period": analysis_period,
            "average_monthly_transactions": len(transactions) / max(len(monthly_groups), 1),
        }
        
        self.logger.info(f"Generated comprehensive summary for {len(transactions)} transactions")
        return comprehensive_summary
    
    def generate_monthly_comparison(
        self,
        transactions: List[TransactionData]
    ) -> Dict[str, Any]:
        """Generate month-over-month comparison analysis.
        
        Args:
            transactions: List of transactions.
            
        Returns:
            Dictionary with month-over-month comparison.
        """
        monthly_groups = self.group_transactions_by_month(transactions)
        monthly_summaries = {}
        
        # Calculate monthly summaries
        for month_key, month_transactions in monthly_groups.items():
            summary = self.calculate_monthly_summary(month_key, month_transactions)
            monthly_summaries[month_key] = summary
        
        # Generate comparisons
        months = sorted(monthly_summaries.keys())
        comparisons = {}
        
        for i in range(1, len(months)):
            current_month = months[i]
            previous_month = months[i - 1]
            
            current_data = monthly_summaries[current_month]
            previous_data = monthly_summaries[previous_month]
            
            # Calculate percentage changes
            credit_change = self._calculate_percentage_change(
                previous_data["total_credits"], current_data["total_credits"]
            )
            debit_change = self._calculate_percentage_change(
                previous_data["total_debits"], current_data["total_debits"]
            )
            transaction_change = self._calculate_percentage_change(
                previous_data["transaction_count"], current_data["transaction_count"]
            )
            
            comparisons[current_month] = {
                "compared_to": previous_month,
                "credit_change_percent": credit_change,
                "debit_change_percent": debit_change,
                "transaction_count_change_percent": transaction_change,
                "net_amount_change": current_data["net_amount"] - previous_data["net_amount"],
            }
        
        return {
            "monthly_summaries": monthly_summaries,
            "month_over_month_comparisons": comparisons,
            "trend_analysis": self._analyze_trends(monthly_summaries),
        }
    
    def _calculate_percentage_change(self, old_value: float, new_value: float) -> float:
        """Calculate percentage change between two values.
        
        Args:
            old_value: Previous value.
            new_value: Current value.
            
        Returns:
            Percentage change.
        """
        if old_value == 0:
            return 0.0 if new_value == 0 else 100.0
        
        return ((new_value - old_value) / old_value) * 100
    
    def _analyze_trends(self, monthly_summaries: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Analyze trends in monthly data.
        
        Args:
            monthly_summaries: Dictionary of monthly summaries.
            
        Returns:
            Dictionary with trend analysis.
        """
        if len(monthly_summaries) < 2:
            return {"insufficient_data": "Need at least 2 months for trend analysis"}
        
        months = sorted(monthly_summaries.keys())
        
        # Analyze credit trends
        credit_values = [monthly_summaries[month]["total_credits"] for month in months]
        credit_trend = "stable"
        if all(credit_values[i] > credit_values[i-1] for i in range(1, len(credit_values))):
            credit_trend = "increasing"
        elif all(credit_values[i] < credit_values[i-1] for i in range(1, len(credit_values))):
            credit_trend = "decreasing"
        
        # Analyze debit trends
        debit_values = [monthly_summaries[month]["total_debits"] for month in months]
        debit_trend = "stable"
        if all(debit_values[i] > debit_values[i-1] for i in range(1, len(debit_values))):
            debit_trend = "increasing"
        elif all(debit_values[i] < debit_values[i-1] for i in range(1, len(debit_values))):
            debit_trend = "decreasing"
        
        return {
            "credit_trend": credit_trend,
            "debit_trend": debit_trend,
            "analysis_period": f"{months[0]} to {months[-1]}",
        }